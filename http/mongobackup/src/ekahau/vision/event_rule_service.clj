(ns ekahau.vision.event-rule-service
  (:require
    [ekahau.vision.engine-service]
    [ekahau.vision.xml.event-rule :as xml]
    [ekahau.vision.database :as database])
  (:use
    [clojure.set :only [difference]]
    [clojure.contrib.logging :only [error]])
  (:import
    java.util.Date))

;;; ENGINE EVENT RULE ;;;

(defn engine-event-rule-parameters-from-area-specification
  [area-specification]
  (condp = (:type area-specification)
    "all" {}
    "selected-zones" {:modelid (:model-id area-specification)
                      :zoneid (xml/ids->str (:ids area-specification))}
    "selected-maps" {:modelid (:model-id area-specification)
                     :mapid (xml/ids->str (:ids area-specification))}))

(defn- engine-event-rule-parameters-from-selected-types-subject-specification
  [db subject-specification]
  (let [ids (->> (:ids subject-specification)
              (map (fn [id] (database/get-entity-by-id db :asset-types id)))
              (map :engine-asset-group-id))]
    {:assetgroupid (sort ids)}))

(defn- engine-event-rule-parameters-from-selected-entities-subject-specification
  [db subject-specification]
  (let [ids (->> (:ids subject-specification)
              (map (fn [id] (database/get-entity-by-id db :assets id)))
              (map :engine-asset-id))]
    {:assetid (sort ids)}))

(defn engine-event-rule-parameters-from-subject-specification
  [db subject-specification]
  (condp = (:type subject-specification)
    "all" {}
    "selected-types" (engine-event-rule-parameters-from-selected-types-subject-specification db subject-specification)
    "selected-entities" (engine-event-rule-parameters-from-selected-entities-subject-specification db subject-specification)))

(defn engine-event-rule-parameters-from-sensor-specification
  [db sensor-specification])

(defn engine-event-rule-parameters-from-alarm-specification
  [db {:keys [high-enable high-value high-time low-enable low-value low-time
              value-unit time-unit hysteresis alarm-on-exit] :as alarm-spec}]
  ; XXX TO-DO convert temp values to Celcius iF ERC doesn't take unit.
  (merge
    (when high-enable {:hightemplimit high-value})
    (when low-enable {:lowtemplimit low-value})))

(def scan-reason-per-button {"button1" 1
                             "button2" 6
                             "menu" 8})

(defn engine-event-rule-parameters-from-button-trigger
  [db button-trigger]
  (merge
    {:scanreason (sort (map scan-reason-per-button (:buttons button-trigger)))}
    (engine-event-rule-parameters-from-subject-specification db (:subject-specification button-trigger))
    (engine-event-rule-parameters-from-area-specification (:area-specification button-trigger))))

(defn- engine-event-rule-parameters-from-area-trigger
  [db area-trigger]
  (let [area-specification (:area-specification area-trigger)
        type (condp = [(:subtype area-trigger) (get-in area-trigger [:area-specification :type])]
               ["enter" "selected-zones"] :enterzoneid
               ["exit"  "selected-zones"] :exitzoneid
               ["enter" "selected-maps"] :entermapid
               ["exit"  "selected-maps"] :exitmapid)]
    (let [result {type (xml/ids->str (:ids area-specification))}]
      (assoc result :modelid
             (or (:model-id area-specification)
                 (ekahau.vision.engine-service/get-active-model-id db))))))

(defn engine-event-rule-parameters-from-battery-trigger
  [db trigger]
  {:lowbatterylimit (str (:min-level trigger))})

(defn engine-event-rule-parameters-from-safety-switch-trigger
  [db trigger]
  (merge
    {:scanreason 10}
    (engine-event-rule-parameters-from-subject-specification db (:subject-specification trigger))
    (engine-event-rule-parameters-from-area-specification (:area-specification trigger))))

(defn engine-event-rule-parameters-from-motion-trigger
  [db trigger]
  (merge
    {:scanreason 2}
    (engine-event-rule-parameters-from-subject-specification db (:subject-specification trigger))
    (engine-event-rule-parameters-from-area-specification (:area-specification trigger))))

(defn engine-event-rule-parameters-from-temperature-trigger
  [db trigger]
  (merge
    (engine-event-rule-parameters-from-subject-specification db (:subject-specification trigger))
    (engine-event-rule-parameters-from-sensor-specification db (:sensor-specification trigger))
    (engine-event-rule-parameters-from-alarm-specification db (:alarm-specification trigger))))

(defn- engine-event-rule-parameters-from-trigger
  [db trigger]
  (condp = (:type trigger)
    "button" (engine-event-rule-parameters-from-button-trigger db trigger)
    "area" (engine-event-rule-parameters-from-area-trigger db trigger)
    "battery" (engine-event-rule-parameters-from-battery-trigger db trigger)
    "motion" (engine-event-rule-parameters-from-motion-trigger db trigger)
    "safety-switch" (engine-event-rule-parameters-from-safety-switch-trigger db trigger)
    "temperature" (engine-event-rule-parameters-from-temperature-trigger db trigger)))

(defn engine-event-rule-parameters-from-event-rule
  [db event-rule]
  (merge
    {:name (:name event-rule)
     :description (:description event-rule)
     :label "vision"}
    (engine-event-rule-parameters-from-trigger db (:trigger event-rule))))

; XXX fix this in case of disabled event rules
(defn- create-engine-event-rule-id!
  [db event-rule]
  (when-let [engine-id (ekahau.vision.engine-service/create-event-rule!
                         (engine-event-rule-parameters-from-event-rule db event-rule))]
    (when (:disabled? event-rule)
      (ekahau.vision.engine-service/disable-event-rule! engine-id))
    engine-id))

;;; Misc ;;;

(defn par-management-rule?
  [event-rule]
  (= "par-level" (-> event-rule :trigger :type)))

;;; EVENT RULE HISTORY ;;;

; Datastructure for an event-rules-history:
; {:id 2512
;  :versions [
;     {}
;     {}
;  ]}

; v-event-rule = versionated event rule (i.e. event-rule entity with versions)
; v-event-rule = event-rule

; event-rule-v = one version of event-rule

(defn- get-event-rule-history
  [db event-rule-id]
  (database/get-entity-by-id db :event-rules-history event-rule-id))

(defn- get-event-rule-version
  [event-rule version-num]
  (when-let [event-rule-v (get (:versions event-rule) version-num)]
    (merge event-rule-v {:id (:id event-rule)
                         :version version-num})))

(defn- get-max-version
  [event-rule]
  (dec (count (:versions event-rule))))

(defn- get-all-versions
  [db event-rule-id]
  (let [event-rule-h (get-event-rule-history db event-rule-id)]
    (map (partial get-event-rule-version event-rule-h)
      (range (inc (get-max-version event-rule-h))))))

(defn- get-latest-event-rule-version
  ([event-rule]
    (get-event-rule-version event-rule
      (get-max-version event-rule)))
  ([db event-rule-id]
    (get-latest-event-rule-version
      (get-event-rule-history db event-rule-id))))

(defn get-next-free-event-rule-id!
  [db]
  (database/get-next-free-entity-id! db :event-rules-history))

(defn assoc-event-rule-id!
  [db event-rule]
  (assoc event-rule :id (get-next-free-event-rule-id! db)))

(defn- put-new-event-rule-history!
  [db event-rule-v]
  (let [event-rule-id (or (:id event-rule-v)
                        (get-next-free-event-rule-id! db))]
  (println "something that is put in db history " event-rule-v) 
    (database/put-entity! db :event-rules-history
      {:id event-rule-id
       :versions [event-rule-v]})))

(defn- put-new-event-rule-version!
  [db event-rule-id event-rule-v]
  (database/update-entity! db :event-rules-history event-rule-id
    (fn [event-rule event-rule-v]
      (let [max-version (get-max-version event-rule)]
        (-> event-rule
          (update-in [:versions max-version]
            assoc :knowledge-end-time
            (:knowledge-begin-time event-rule-v))
          (update-in [:versions] conj event-rule-v))))
    event-rule-v))

;;; New / Edit rule / Enable / Disable ;;;

(defn- get-engine-id
  ([db event-rule-id]
    (:engine-id (get-latest-event-rule-version db event-rule-id)))
  ([event-rule-v]
    (:engine-id event-rule-v)))

(defn vision-only-rule?
  ([db event-rule-id]
    (vision-only-rule?
      (get-latest-event-rule-version db event-rule-id)))
  ([event-rule-v]
    (par-management-rule? event-rule-v)))

(defn- assoc-knowledge-begin-time
  [event-rule-stub]
  (assoc event-rule-stub :knowledge-begin-time (.getTime (Date.))))

(defn- assoc-disabled-if-required
  [event-rule-stub]
  (if-not (nil? (:disabled? event-rule-stub))
    event-rule-stub
    (assoc event-rule-stub :disabled? false)))

(defn- assoc-engine-event-rule-if-required!
  [db event-rule-stub]
  (if (or (vision-only-rule? event-rule-stub)
        (contains? event-rule-stub :engine-id))
    event-rule-stub
    (assoc event-rule-stub :engine-id
      (create-engine-event-rule-id! db event-rule-stub))))

(defn- assoc-id-if-required!
  [db event-rule-stub]
  (if-not (contains? event-rule-stub :id)
    (assoc-event-rule-id! db event-rule-stub)
    event-rule-stub))

(defn- dissoc-version-if-required
  [db event-rule-stub]
  (if (contains? event-rule-stub :version)
    (dissoc event-rule-stub :version)
    event-rule-stub))

(defn- complete-event-rule-stub
  [db event-rule-stub]
  (->> event-rule-stub
    (assoc-id-if-required! db)
    (dissoc-version-if-required db)
    (assoc-knowledge-begin-time)
    (assoc-disabled-if-required)
    (assoc-engine-event-rule-if-required! db)))

(def event-rule-objs (atom {}))

(defn- get-event-rule-obj
  [event-rule-id]
  (if (contains? @event-rule-objs event-rule-id)
    (get @event-rule-objs event-rule-id)
    (get (swap! event-rule-objs assoc event-rule-id (Object.))
      event-rule-id)))

(defmacro with-event-rule-lock
  [event-rule-id & body]
  `(locking (get-event-rule-obj ~event-rule-id)
     ~@body))

(defn- change-erc-rule-if-required!
  [db event-rule-id erc-fn]
  (let [event-rule-lt (get-latest-event-rule-version db event-rule-id)]
    (when-not (vision-only-rule? event-rule-lt)
      (erc-fn (get-engine-id event-rule-lt)))))

(defn- disable-erc-rule-if-required!
  [db event-rule-id]
  (change-erc-rule-if-required! db event-rule-id
    ekahau.vision.engine-service/disable-event-rule!))

(defn- enable-erc-rule-if-required!
  [db event-rule-id]
  (change-erc-rule-if-required! db event-rule-id
    ekahau.vision.engine-service/enable-event-rule!))

(defn- event-rule-disabled?
  ([db event-rule-id]
    (event-rule-disabled? (get-latest-event-rule-version db event-rule-id)))
  ([event-rule-v]
    (:disabled? event-rule-v)))

; This function returns true for event rule versions
; which are expected to produce events
(defn- event-rule-active?
  ([db event-rule-id]
    (event-rule-active? (get-latest-event-rule-version db event-rule-id)))
  ([event-rule-v]
    (and (nil? (:knowledge-end-time event-rule-v)) ;latest version 
      (not (event-rule-disabled? event-rule-v)))))

(defn- all-erc-rules-inactive?
  [erc-rule-ids]
  (every? false? (map ekahau.vision.engine-service/event-rule-active? erc-rule-ids)))

(defn- event-rules-consistent-wrt-activeness?
  [db event-rule-id]
  (let [event-rule-versions (get-all-versions db event-rule-id)
        all-engine-ids (set (remove nil? (map get-engine-id event-rule-versions)))
        latest-engine-id (get-engine-id db event-rule-id)]
    (if (event-rule-active? db event-rule-id)
      (if (nil? latest-engine-id)
        (all-erc-rules-inactive? all-engine-ids)
        (and (ekahau.vision.engine-service/event-rule-active? latest-engine-id)
          (all-erc-rules-inactive? (difference all-engine-ids #{latest-engine-id}))))
      (all-erc-rules-inactive? all-engine-ids))))

; XXX TO-DO : run this on a periodic basis on a separate thread?
(defn- perform-event-rule-active-consistency-check
  [db event-rule-id]
  (when-not (event-rules-consistent-wrt-activeness? db event-rule-id)
    (error (str "Event rule Id " event-rule-id " fails active consistency check."))))

; This function must be "synchronized" on event-rule-id
(defn- disable-event-rule-sequential!
  [db event-rule-id]
  (if-not (event-rule-disabled? db event-rule-id)
    (do
      (disable-erc-rule-if-required! db event-rule-id)
      (let [db (put-new-event-rule-version! db event-rule-id
                 (complete-event-rule-stub db
                   (assoc (get-latest-event-rule-version db event-rule-id)
                     :disabled? true)))]
        (perform-event-rule-active-consistency-check db event-rule-id)
        db))
    db))

; This function must be "synchronized" on event-rule-id
(defn- enable-event-rule-sequential!
  [db event-rule-id]
  (if (event-rule-disabled? db event-rule-id)
    (do
      (enable-erc-rule-if-required! db event-rule-id)
      (let [db (put-new-event-rule-version! db event-rule-id
                 (complete-event-rule-stub db
                   (assoc (get-latest-event-rule-version db event-rule-id)
                     :disabled? false)))]
        (perform-event-rule-active-consistency-check db event-rule-id)
        db))
    db))

; This function must be "synchronized" on event-rule-id
(defn- update-event-rule-sequential!
  [db event-rule-id event-rule-stub]
  (disable-erc-rule-if-required! db event-rule-id)
  (let [db (put-new-event-rule-version! db event-rule-id
             (complete-event-rule-stub db event-rule-stub))]
    (perform-event-rule-active-consistency-check db event-rule-id)
    db))

; Updating event rule consists of following steps
; 1. Disabling existing ERC rule
; 2. Creating a new ERC rule
; 3. Creating a new version in Vision
; Clearly these steps cannot be handled in a transaction by the Clojure STM
; Thus we use an event-rule monitor (unique for event-rule-id)
(defn update-event-rule!
  [db event-rule-id event-rule-stub]
  (with-event-rule-lock event-rule-id
    (update-event-rule-sequential!
      db event-rule-id event-rule-stub)))

; Disabling event rule consists of following steps
; 1. Disabling existing ERC rule
; 2. Creating a new version in Vision
; Clearly these steps cannot be handled in a transaction by the Clojure STM
; Thus we use an event-rule monitor (unique for event-rule-id)
(defn disable-event-rule!
  [db event-rule-id]
  (with-event-rule-lock event-rule-id
    (disable-event-rule-sequential! db event-rule-id)))

; Enabling event rule consists of following steps
; 1. Enabling existing ERC rule
; 2. Creating a new version in Vision
; Clearly these steps cannot be handled in a transaction by the Clojure STM
; Thus we use an event-rule monitor (unique for event-rule-id)
(defn enable-event-rule!
  [db event-rule-id]
  (with-event-rule-lock event-rule-id
    (enable-event-rule-sequential! db event-rule-id)))

(defn put-new-event-rule!
  [db event-rule-stub]
  (let [event-rule-stub (assoc-id-if-required! db event-rule-stub)
        complete-event (complete-event-rule-stub db event-rule-stub)
        db (put-new-event-rule-history! db complete-event)]
    (println "the COMPLETE rule to be put in the db is " complete-event) 
    (perform-event-rule-active-consistency-check db (:id event-rule-stub))
    db))

(defn put-new-event-rules!
  [db event-rule-stubs]
  (reduce put-new-event-rule! db event-rule-stubs))

;;; Getters ;;;

(defn- assoc-active
  [event-rule-v]
  (when event-rule-v
    (assoc event-rule-v :active? (event-rule-active? event-rule-v))))

(defn get-event-rules
  [db]
  (map (comp assoc-active get-latest-event-rule-version)
    (database/get-entities db :event-rules-history)))

(defn get-event-rule-by-id
  ([db event-rule-id]
    (assoc-active
      (get-latest-event-rule-version db event-rule-id)))
  ([db event-rule-id version-num]
    (assoc-active
      (get-event-rule-version
        (get-event-rule-history db event-rule-id)
        version-num))))

(defn get-event-rule-by-id-all-versions
  [db event-rule-id]
  (map assoc-active (get-all-versions db event-rule-id)))

(defn get-event-rule-by-engine-rule-id
  [db engine-rule-id]
  (some #(when (= engine-rule-id (get-engine-id %)) %) 
    (get-event-rules db)))
