(ns ekahau.vision.event-service
  (:require
    [ekahau
     [UID :as uid]
     [email :as email]]
    [ekahau.engine.cmd]
    [ekahau.vision
     [database :as database]
     [engine-service]]
    [ekahau.engine.events :as erc])
  (:use
    [clojure.set :only [union]]
    [clojure.contrib
     [logging :only [debug warn error info]]
     [seq-utils :only [indexed]]
     [str-utils :only [str-join]]
     [trace :only [trace]]]
    [clojure.pprint :only [pprint]]
    [ekahau.vision.properties :only [load-email-config!]]
    [ekahau.vision.model.asset :only (get-asset-by-id
                                      get-asset-type-by-id
                                      get-asset-by-engine-asset-id
                                      get-short-asset-string
                                      get-assets-of-type-and-descendants)]
    [ekahau.vision.event-rule-service :only [get-event-rule-by-id
                                             get-event-rule-by-engine-rule-id]]
    [ekahau.vision.model.positions :only [get-map-by-id
                                          get-zone-by-id
                                          get-model-by-id
                                          get-floor-by-id
                                          get-building-by-id
                                          get-floor-from-map-id
                                          get-building-from-map-id
                                          get-asset-position-observation
                                          form-detailed-position]]
    [ekahau.vision.event.emergin-action :only [perform-emergin-action]]
    [ekahau.vision.model.zone-grouping :only [get-zone-grouping-info-for-zone-id]]
    [ekahau.string :only [parse-id parse-double parse-long truncate-str]])
  (:import
    [java.util Date]
    [java.util.concurrent Executors TimeUnit]))

;;; Helpers ;;;

(defn get-events
  [db & {:as opts}]
  (database/get-entities db :events opts))

(defn get-event-by-id
  [db event-id]
  (database/get-entity-by-id db :events event-id))

(defn get-messages
  [db]
  (database/get-entities db :messages))

(defn get-message-by-id
  [db msg-id]
  (database/get-entity-by-id db :messages msg-id))

(defn put-event!
  [db event]
  (dosync
    (let [event-with-id (database/assoc-new-entity-id! db :events event)]
      (database/put-entity! db :events event-with-id))))

;;; Entity construction ;;;

(defn- construct-entity
  ([entity]
    (struct-map entity))
  ([entity & maps]
    (apply struct-map entity (mapcat identity (apply merge maps)))))

(defn construct-event
  [& maps]
  (apply construct-entity database/event maps))

(defn construct-message
  [& maps]
  (apply construct-entity database/message maps))

;; Tag message ;; 

(defmulti get-tag-ids-by-target 
  {:private true}
  (fn [db target originator-tag-id] (:type target)))

(defn- get-engine-asset-id-set-from-assets
  [assets]
  (->> assets
    :engine-asset-id
    (remove nil?)
    (set)))

(defn- get-engine-asset-id-set-from-db
  [db]
  (get-engine-asset-id-set-from-assets (database/get-entities db :assets)))

(defn- get-tag-asset-id-pairs
  []
  (->> (ekahau.engine.cmd/epe-get-tags)
    (map (fn [{{:keys [tagid assetid]} :properties}]
           [(parse-id tagid)
            (parse-id assetid)]))
    (filter (fn [[tag-id asset-id]]
              (and tag-id asset-id)))))

(defn- get-engine-tag-ids-of-engine-asset-ids
  [engine-asset-ids-set]
  (->> (get-tag-asset-id-pairs)
    (filter (fn [[tag-id asset-id]] (engine-asset-ids-set asset-id)))
    (map (fn [[tag-id _]] tag-id))))

(defn- exclude-original-if-requested
  [originator-tag-id target tag-ids]
  (if (= true (:exclude-originator target))
    (remove #(= originator-tag-id %) tag-ids)
    tag-ids))

(defmethod get-tag-ids-by-target "originator-tag"
  [db target originator-tag-id]
  originator-tag-id)

(defmethod get-tag-ids-by-target "all-display-tags"
  [db target originator-tag-id]
  (get-engine-tag-ids-of-engine-asset-ids (get-engine-asset-id-set-from-db db)))

(defn- get-all-assets-of-types
  [db asset-type-id-set]
  (->> asset-type-id-set
    (map (partial get-assets-of-type-and-descendants db))
    (apply union)))

(defmethod get-tag-ids-by-target "asset-groups"
  [db target originator-tag-id]
  (-> (get-all-assets-of-types db (:ids target))
    (get-engine-asset-id-set-from-assets)
    (get-engine-tag-ids-of-engine-asset-ids)))

(defmethod get-tag-ids-by-target "assets-tag"
  [db target originator-tag-id]
  (let [asset-id-set (set (:ids target))
        assets (filter #(asset-id-set (:engine-asset-id %)) (database/get-entities db :assets))]
    (->> (database/get-entities db :assets)
      (filter #(asset-id-set (:id %)))
      (get-engine-asset-id-set-from-assets)
      (get-engine-tag-ids-of-engine-asset-ids))))

(defn- format-tag-msg-value
  [s max-length]
  (if s
    (truncate-str s max-length "..")
    "Unknown"))

(defn create-instant-tag-message
  ([values]
    (create-instant-tag-message values {}))
  ([{:keys [message-text zone-name]}
    {:keys [message-max-length zone-name-max-length]
     :or {message-max-length 42, zone-name-max-length 45}}]
    (str (format-tag-msg-value message-text message-max-length) "\n"
      "Zone:" (format-tag-msg-value zone-name zone-name-max-length))))

(defn create-tag-message
  ([values]
    (create-tag-message values {}))
  ([{:keys [message-text map-name zone-name tag-name]}
    {:keys [message-max-length map-name-max-length zone-name-max-length tag-name-max-length]
     :or {message-max-length 42, map-name-max-length 45, zone-name-max-length 45, tag-name-max-length 14}}]
    (str (format-tag-msg-value message-text message-max-length) "\n" 
      "Zone:" (format-tag-msg-value zone-name zone-name-max-length) "\n"
      "Map:" (format-tag-msg-value map-name map-name-max-length) "\n"
      "From:" (format-tag-msg-value tag-name tag-name-max-length))))

(defn- get-zone-name
  [db zone-id]
  (if zone-id
    (:name (database/get-entity-by-id db :zones zone-id))
    "Unknown"))

(defn- get-map-name
  [db map-id]
  (if map-id
    (:name (database/get-entity-by-id db :maps map-id))
    "Unknown"))

(defn- event-originator-str-by-engine-asset-id
  [db engine-asset-id]
  (let [asset (get-asset-by-engine-asset-id db engine-asset-id)]
    (get-short-asset-string db asset)))

(defn- event-originator-str-from-event
  [db event]
  (event-originator-str-by-engine-asset-id db (:engine-asset-id event)))

(defn- create-tag-message-for-action
  [db
   {:keys [instant message] :as action}
   {{{:keys [map-id zone-id]} :position} :position-observation originator-tag :engine-tag-id :as event}]
  (if instant
    (create-instant-tag-message {:message-text message
                                 :zone-name (get-zone-name db zone-id)})
    (create-tag-message {:message-text message
                         :zone-name (get-zone-name db zone-id)
                         :map-name (get-map-name db map-id)
                         :tag-name (event-originator-str-from-event db event)})))

(defn- append-row
  [rows & text]
  (if (second text)
   (conj rows (str-join " " text))
   rows))

(defn- rule-type-str-from-rule
  [event-rule]
  (condp = (-> event-rule :trigger :type)
    "button"         "Button Press"
    "safety-switch"  "Safety Switch"
    "area" (condp = (-> event-rule :trigger :subtype)
             "enter" "Area Enter"
             "exit"  "Area Exit")
    "battery"        "Low Battery"
    "motion"         "Motion"
    "telemetry"      "Telemetry"
    "Unknown"))

(defn- map-and-zone-str-from-event
  [db event]
  (let [position (-> event :position-observation :position)] 
    (str (get-map-name db (-> position :map :id)) "/"
      (get-zone-name db (-> position :zone :id)))))

(defn- combine-rows
  [rows format]
  (str-join
    (if format
      "\n"
      " ")
    rows))

(defn- create-email-content
  [db event-rule event]
  (-> []
    (append-row "")
    (append-row "")
    (append-row "EVENT DETAILS:")
    (append-row "Event Type:" (rule-type-str-from-rule event-rule))
    (append-row "Event Originator:" (event-originator-str-from-event db event))
    (append-row "Map/Zone:" (map-and-zone-str-from-event db event))
    (append-row "Time: " (.toString (Date. (long (:timestamp event)))))
    (append-row "Battery Level: " (if-let [battery (:battery event)] (str battery "%")))
    (combine-rows (-> event-rule :action :do-not-format not))))

(defonce email-agent (agent nil))

(defn- empty-str?
  [^String s]
  (or (nil? s) (= 0 (.length s))))

(defn- create-email-message
  [subject from recipients text]
  {:from from
   :recipients recipients
   :subject subject
   :text text})

(defn- send-email!
  [subject recipients text]
  (send email-agent
    (fn [_]
      (try
        (let [{:keys [host port use-authentication
                      username password sender]} (load-email-config!)
              session (email/create-session host port use-authentication)
              messages [(create-email-message subject sender recipients text)]]
          (if (or (empty-str? username)
                (empty-str? password))
            (email/send-email! session messages)
            (email/send-email! session messages username password)))
        (catch Exception e
          (error "Sending email failed" e))))))

;; EMsg -> Event ;;

(defn- position-from-emsg
  [{{:keys [posmapid posx posy poszoneid posmodelid]} :properties}]
  (struct-map database/detailed-position
    :map   {:id (parse-id posmapid)}
    :point [(parse-double posx) (parse-double posy)]
    :zone  {:id (parse-id poszoneid)}
    :model {:id (parse-id posmodelid)}))

(defn- engine-event-rule-id-from-emsg
  [{{:keys [ruleid]} :properties}]
  (parse-id ruleid))

(defn- timestamp-from-emsg
  [{{:keys [postime]} :properties}]
  (parse-long postime))

(defn- position-observation-from-emsg
  [db event-emsg]
  (struct-map database/position-observation
    :position (position-from-emsg event-emsg)
    :timestamp (timestamp-from-emsg event-emsg)))

(defn- engine-asset-id-from-emsg
  [{{:keys [assetid]} :properties}]
  (parse-id assetid))

(defn- engine-tag-id-from-emsg
  [{{:keys [tagid]} :properties}]
  (parse-id tagid))

(defmulti extract-keys-from-emsg
  "[db event-emsg]
   Different event rules may have different emsg formats"
  ^{:private true}
   #(:type %2))

(defmethod extract-keys-from-emsg :default
  [db event-emsg]
  {:position-observation (position-observation-from-emsg db event-emsg)
   :engine-asset-id (engine-asset-id-from-emsg event-emsg)
   :engine-tag-id (engine-tag-id-from-emsg event-emsg)
   :timestamp (timestamp-from-emsg event-emsg)})

(defmethod extract-keys-from-emsg "BATTERYLOW"
  [db event-emsg]
  (let [engine-asset-id (engine-asset-id-from-emsg event-emsg)
        asset-id (:id (get-asset-by-engine-asset-id db
                        engine-asset-id))
        engine-tag-id (engine-tag-id-from-emsg event-emsg)
        asset-pos-obs (:position-observation (get-asset-position-observation db asset-id))]
    (when-not asset-pos-obs
      (warn (str "Could not find asset-pos-obs for asset-id:" asset-id " to get pos for battery rule event")))
    {:engine-asset-id engine-asset-id
     :engine-tag-id engine-tag-id
     :position-observation {:position (struct-map database/detailed-position
                                        :map   {:id (:map-id (:position asset-pos-obs))}
                                        :point (:point (:position asset-pos-obs))
                                        :zone  {:id (:zone-id (:position asset-pos-obs))}
                                        :model {:id (:model-id (:position asset-pos-obs))})
                            :timestamp (:timestamp asset-pos-obs)}
     :timestamp (parse-long (:eventtime (:properties event-emsg)))}))

;; Add Message ;;

(defmulti create-message-recipients
  (fn [db spec] (:type spec)))

(defmethod create-message-recipients "selected-users"
  [db {recipients :recipients :as recipient-spec}]
  (let [recipients (set recipients)]
    (->> (database/get-entities db :users)
      (filter #(contains? recipients (:uid %)))
      (map :id)
      (set)
      (seq))))

(defmethod create-message-recipients "all-users"
  [db spec]
  (->> (database/get-entities db :users)
    (map :id)
    (set)
    (seq)))

(defn- send-new-message!
  [db message]
  (dosync
    (let [message (database/assoc-new-entity-id! db :messages message)]
      (database/put-entity! db :messages message)
      message)))

(defn- add-new-message-from-event!
  [db event-rule event type-of-msg]
  (send-new-message! db
    (construct-message
      {:type type-of-msg
       :event-id (:id event)
       :message (-> event-rule :action :title)
       :read-by []
       :recipients (create-message-recipients db 
                     (get-in event-rule [:action :target]))
       :timestamp (:timestamp event)})))

;; Comment/Accept/Decline/Close Event

(defn- add-comment-in-db!
  [db event comment]
  (put-event! db (assoc event :comments
                   (conj (:comments event []) comment))))

(defn comment-on-event!
  [db event-id {:keys [user time text] :as comment}]
  (if-let [event (get-event-by-id db event-id)]
    (dosync
      (add-comment-in-db! db event comment)
      (send-new-message! db
        (construct-message
          {:type "comment-on-event"
           :event-id (:id event)
           :message text
           :read-by []
           :recipients (:msg-recp event)
           :timestamp (:timestamp event)})))
    (throw (Exception. (str "Can't put comment as there is no event with id " event-id)))))

(defn- close-event-in-db!
  [db event closing-remark]
  (put-event! db (-> event
                   (assoc :closing-remarks
                     (conj (:closing-remarks event []) closing-remark))
                   (assoc :closed? true)
                   (assoc :closing-time (:time closing-remark)))))

(defn close-event!
  [db event-id {:keys [user time text] :as closing-remark}]
  (if-let [event (get-event-by-id db event-id)]
    (dosync
      (close-event-in-db! db event closing-remark)
      (send-new-message! db
        (construct-message
          {:type "closing-remark-on-event"
           :event-id (:id event)
           :message text
           :read-by []
           :recipients (:msg-recp event)
           :timestamp (:timestamp event)})))
    (throw (Exception. (str "Can't close as there is no event with id " event-id)))))

(defn- accept-event-in-db!
  [db event accepted]
  (put-event! db (assoc event :accepted-by
                   (conj (:accepted-by event []) accepted))))

(defn accept-event!
  [db event-id {:keys [user time] :as accepted}]
  (if-let [event (get-event-by-id db event-id)]
    (dosync
      (accept-event-in-db! db event accepted)
      (send-new-message! db
        (construct-message
          {:type "accept-event"
           :event-id (:id event)
           :message (str "Event " event-id " accepted by " (:name user))
           :read-by []
           :recipients (:msg-recp event)
           :timestamp (:timestamp event)})))
    (throw (Exception. (str "Can't accept as there is no event with id " event-id)))))

(defn- decline-event-in-db!
  [db event declined]
  (put-event! db (assoc event :declined-by 
                   (conj (:declined-by event []) declined))))

(defn decline-event!
  [db event-id {:keys [user time] :as declined}]
  (if-let [event (get-event-by-id db event-id)]
    (dosync
      (decline-event-in-db! db event declined)
      (send-new-message! db
        (construct-message
          {:type "decline-event"
           :event-id (:id event)
           :message (str "Event " event-id " declined by " (:name user))
           :read-by []
           :recipients (:msg-recp event)
           :timestamp (:timestamp event)})))
    (throw (Exception. (str "Can't decline as there is no event with id " event-id)))))

;; Read / Unread Messages ;;

(defn mark-msg-unread!
  [db user-id msg-id]
  (database/update-entity! db :messages msg-id
    update-in [:read-by] #(seq (disj (set %1) %2)) user-id))

(defn mark-msg-read!
  [db user-id msg-id]
  (database/update-entity! db :messages msg-id
    update-in [:read-by] #(seq (conj (set %1) %2)) user-id))

;; Get mailbox for user ;;

(defn read-by-user?
  ([db user-id msg-id]
    (read-by-user? user-id (get-message-by-id db msg-id)))
  ([user-id msg]
    (not (nil? (some #(= % user-id) (:read-by msg))))))

(defn- mark-user-read-status
  [db user-id msgs]
  (map #(assoc % :read? (read-by-user? user-id %)) msgs))

(defn- mk-read-search-param
  [db user-id nots-read? alerts-read?]
  (case (database/db-type db :messages)
    :in-memory #(let [read? (read-by-user? user-id %)]
                  (or
                    (and (= "alert" (:type %))
                      (or (nil? alerts-read?) (= alerts-read? read?)))
                    (and (not (= "alert" (:type %)))
                      (or (nil? nots-read?) (= nots-read? read?)))))
    :mongodb (let [alert-doc (merge {:type "alert"}
                               (when-not (nil? alerts-read?)
                                 (if alerts-read?
                                   {:read-by user-id}
                                   {:read-by {"$ne" user-id}})))
                   nots-doc (merge {:type {"$ne" "alert"}}
                              (when-not (nil? nots-read?)
                                (if nots-read?
                                  {:read-by user-id}
                                  {:read-by {"$ne" user-id}})))]
               {"$or" [alert-doc nots-doc]})))

(defn get-user-mailbox
  ([db user-id]
    (let [messages (mark-user-read-status db user-id
                     (database/search-entities db :messages
                       [["SOME=" [:recipients] user-id]]
                       {:order-by [[[:id] -1]]}))]
      {:messages messages
       :maxMsgId (:id (first messages))}))
  ([db user-id min-msg-id inclusive?]
    (let [messages (mark-user-read-status db user-id
                     (database/search-entities db :messages
                       [["SOME=" [:recipients] user-id]
                        [(if inclusive? ">=" ">") [:id] min-msg-id]]
                       {:order-by [[[:id] -1]]}))]
      {:messages messages
       :maxMsgId (:id (first messages))}))
  ([db user-id min-msg-id inclusive? nots-read? alerts-read? & {:as opts}]
    (let [read-search-param (mk-read-search-param db user-id
                              nots-read? alerts-read?)
          cursor-opts (merge {:order-by [[[:id] 1]]}
                        opts)
          messages (mark-user-read-status db user-id
                     (database/search-entities db :messages
                       [["SOME=" [:recipients] user-id]
                        [(if inclusive? ">=" ">") [:id] min-msg-id]
                        read-search-param]
                       cursor-opts))
          messages (case (:order-by cursor-opts)
                     [[[:id] 1]]  (reverse messages)
                     [[[:id] -1]] messages)]
      {:messages messages
       :maxMsgId (:id (first messages))})))

(defn get-last-few-user-mailbox
  ([db user-id n]
    (get-last-few-user-mailbox db user-id n nil nil))
  ([db user-id n nots-read? alerts-read?]
    (get-user-mailbox db user-id uid/MIN-UID false
      nots-read? alerts-read? :limit n :order-by [[[:id] -1]])))

(defn- get-user-mailbox-helper
  [db user-id {:keys [nots-read? alerts-read? limit]}
   extra-search-params cursor-opts]
  (mark-user-read-status db user-id
    (database/search-entities db :messages
      (concat
        [["SOME=" [:recipients] user-id]
         (mk-read-search-param db user-id
           nots-read? alerts-read?)]
        extra-search-params)
      (merge
        {:order-by [[[:id] -1]]}
        (when limit {:limit limit})
        cursor-opts))))

(defn get-user-mailbox-latest
  [db user-id {:keys [nots-read? alerts-read? limit] :as opts}]
  (get-user-mailbox-helper db user-id opts nil nil))

(defn get-user-mailbox-prev
  [db user-id {:keys [nots-read? alerts-read? limit older-than] :as opts}]
  (get-user-mailbox-helper db user-id opts [["<" [:id] older-than]] nil))

(defn get-user-mailbox-next
  [db user-id {:keys [nots-read? alerts-read? limit newer-than] :as opts}]
  (reverse
    (get-user-mailbox-helper db user-id opts [[">" [:id] newer-than]]
      {:order-by [[[:id] 1]]})))

;; Event actions ;;

(defmulti perform-event-rule-action!
  {:private true}
  (fn [db event-rule event] 
    (let [return (-> event-rule :action :type)] 
      (println "the dispatch value is " return)
      return)))

;pranav 
(defmethod perform-event-rule-action! "httpget" 
  [db event-rule event]
  ) 

(defmethod perform-event-rule-action! :default
  [db event-rule event]
  (warn (format "Unknown kind of event: %s %s" event event-rule)))

(defmethod perform-event-rule-action! "composite"
  [db event-rule event]
  (vec
    (remove nil?
      (reduce #(concat %1
                 (perform-event-rule-action! db
                   (assoc event-rule :action %2) event))
        []
        (-> event-rule :action :components)))))

(defmethod perform-event-rule-action! "none"
  [db event-rule event])

(defmethod perform-event-rule-action! "tag-message"
  [db event-rule event]
  (let [action (:action event-rule)
        target (:target action)
        originator-tag (:engine-tag-id event)
        tag-ids (exclude-original-if-requested originator-tag target (get-tag-ids-by-target db target originator-tag))
        message (create-tag-message-for-action db action event)]
    (if (:instant action)
      (ekahau.vision.engine-service/send-instant-message! tag-ids message)
      (ekahau.vision.engine-service/send-text-message! tag-ids message))
    [{:type "tag-message" :tag-ids tag-ids :message message}]))

(defmethod perform-event-rule-action! "emergin-message"
  [db event-rule event]
  (let [{:keys [message source-id sensitivity]} (:action event-rule)]
    (perform-emergin-action
     {:source-id source-id
      :sensitivity sensitivity
      :text (.replaceAll
             (str message (create-email-content db event-rule event))
             "\n" " ")})))

(defmethod perform-event-rule-action! "email"
  [db event-rule event]
  (let [{:keys [message-subject message-text do-not-format recipients]} (:action event-rule)
        text (combine-rows [message-text (create-email-content db event-rule event)] (not do-not-format))]
    (send-email! message-subject recipients text)
    [{:type "email" :recipients recipients :subject message-subject :text text}]))



(defmethod perform-event-rule-action! "notification"
  [db event-rule event]
  (let [message (add-new-message-from-event! 
                  db event-rule event "notification")]
    [{:type "message" :id (:id message)}]))

(defmethod perform-event-rule-action! "alert"
  [db event-rule event]
  (let [message (add-new-message-from-event! 
                  db event-rule event "alert")]
    [{:type "message" :id (:id message)}]))



;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Detailing the event ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- asset-type-details-from-asset
  [db asset]
  (select-keys
    (get-asset-type-by-id db
      (:asset-type-id asset))
    [:id :name]))

(defn- asset-type-details
  [db event]
  (asset-type-details-from-asset db
    (get-asset-by-id db
      (get-in event [:asset-info :id]))))

(defn- zone-grouping-details
  [db event]
  (get-zone-grouping-info-for-zone-id db
    (get-in event [:position-observation :position :zone :id])))

(def *latest-details-spec*
  ^{:private true}
  {[:asset-type-info] asset-type-details
   [:position-observation :position :zone-grouping] zone-grouping-details})

(defn get-latest-event-details
  ([db event _ detailing-specs]
    (when event
      (reduce (fn [e [ks f]]
                (assoc-in e ks (f db e)))
        event
        detailing-specs)))
  ([db event keyseq]
    (get-latest-event-details db event nil
      (select-keys *latest-details-spec* keyseq)))
  ([db event]
    (get-latest-event-details db event nil *latest-details-spec*)))

(defn event-rule-details
  [event-rule]
  (select-keys event-rule [:id :version :name :engine-id]))

;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Top level operations ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn handle-event!
  [db event-rule event]
  (let [actions (perform-event-rule-action! db event-rule event)
        msg-recp (distinct
                   (remove nil?
                     (mapcat #(when (= (:type %) "message")
                                (:recipients
                                  (get-message-by-id db (:id %))))
                       actions)))
        event (construct-event event {:actions actions
                                      :msg-recp msg-recp})]
    (println "the event to be put in the db is  : " event) 
    (debug (str "Generated event:\n" (with-out-str (pprint event))))
    (put-event! db event)))

(defn handle-event-emsg!
  [db event-emsg]
  (println "handle-event-emsg! is being called with the event-emsg: " event-emsg) 
  (debug (str "Start handling event: " event-emsg))
  (try
    (let [rule-id (engine-event-rule-id-from-emsg event-emsg)
          emsg-keys (extract-keys-from-emsg db event-emsg)]
      (if-let [event-rule (get-event-rule-by-engine-rule-id db rule-id)]
        (if-let [asset (get-asset-by-engine-asset-id db
                         (:engine-asset-id emsg-keys))]
          (let [event-id (database/get-next-free-entity-id! db :events)
                event (->
                        (construct-event
                          emsg-keys
                          {:id event-id
                           :type "engine-event"
                           :event-rule-info (event-rule-details event-rule)
                           :asset-info asset
                           :asset-type-info (asset-type-details-from-asset db asset)
                           :closed? false})
                        (update-in [:position-observation :position]
                          (partial form-detailed-position db)))]
            (handle-event! db event-rule event))
          (do
            (warn (str "Could not find an asset with engine-asset-id for event" event-emsg))
            db))
        (do
          (debug (str "Could not find an event-rule for event " event-emsg))
          db)))
    (catch Throwable t
      (do
        (error (str "Error handling event: " event-emsg) t)
        db))
    (finally
      (debug (str "Completed handling event: " event-emsg)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;; Event Management ;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Create Event Manager ;;

(defn create-event-manager
  [erc-config]
  {:event-tracker (erc/create-event-tracking-engine
                    erc-config)
   :event-handler (Executors/newSingleThreadExecutor)})

;; Start Managing Events ;;

(defn start-tracking-events!
  [{:keys [event-tracker]}]
  (info "Starting ERC events tracking ...")
  (erc/start-tracking-events! event-tracker))

(defn start-handling-events!
  [{:keys [event-tracker event-handler]} db {:keys [model-change-handler!] :as opts}]
  (try
    (.execute event-handler
      (bound-fn []
        (info "Starting event handler ...")
        (try
          ;; It is critical not to hold on to the head of the event-seq
          (doseq [emsg (erc/event-seq event-tracker)]
            (try
              (debug (str "Handling event: " emsg))
              (if (= (:type emsg) "ACTIVEMODELCHANGED")
                (model-change-handler! emsg)
                (handle-event-emsg! db emsg))
              (catch Exception e
                (error "Error in event handler" e))))
          (catch Exception e
            (error "Error in event handler" e)))))
    (catch Exception e
      (error "Error starting event manager" e))))

;; Stop Managing Events ;;

(defn stop-tracking-events!
  [{:keys [event-tracker]}]
  (info "Stopping ERC event tracking ..")
  (erc/stop-tracking-events! event-tracker))

(defn stop-handling-events!
  [{:keys [event-handler]}]
  (try
    (info "Awaiting event-handler ..")
    (.shutdown event-handler)
    (while (not (.isTerminated event-handler))
      (.awaitTermination event-handler 1 TimeUnit/SECONDS))
    (info "Stopped handling events.")
    (catch Exception e
      (error "Error stopping event manager" e))))
