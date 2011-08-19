(ns ekahau.vision.xml.event-rule
  (:require 
    [ekahau.entity :as entity]
    [ekahau.vision.database :as database]
    [ekahau.vision.model.event-rule :as model])
  (:use
    [clojure.contrib.condition :only [raise]]
    [clojure.contrib.str-utils :only [re-split str-join]]
    [clojure.contrib.trace :only [trace]]
    [ekahau.string :only [parse-id parse-boolean parse-number parse-double string->id-set]]
    [ekahau.vision.xml.helper :only [get-first-child-element]]
    [ekahau.vision.model.event-rule :only [composite-action tag-message-action message-target-entities alert-action
                                           emergin-message-action notification-action user-target-entities email-action httpget-action]]))

(defn- id-set-from-xml
  [xml attr]
  (string->id-set (-> xml :attrs attr)))

(defn ids->str
  [ids]
  (str-join "," (sort ids)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; XML -> :subject-specification ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- xml->all-subject-specification
  [xml]
  {:type "all"})

(defn- xml->selected-types-subject-specification
  [xml]
  {:type "selected-types"
   :ids (seq (id-set-from-xml xml :selectedIDs))})

(defn- xml->selected-entities-subject-specification
  [xml]
  {:type "selected-entities"
   :ids (seq (id-set-from-xml xml :selectedIDs))})

(defn xml->subject-specification
  [xml]
  (when xml
    (condp = (-> xml :attrs :type)
      "all" (xml->all-subject-specification xml)
      "selectedTypes" (xml->selected-types-subject-specification xml)
      "selectedEntities" (xml->selected-entities-subject-specification xml)
      (raise :type :unknown-subject-specification-type))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; :subject-specification -> XML ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- all-subject-specification->xml
  [subject-specification]
  {:tag :subjectSpecification
   :attrs {:type "all"}
   :content nil})

(defn- selected-types-subject-specification->xml
  [subject-specification]
  {:tag :subjectSpecification
   :attrs {:type "selectedTypes"
           :selectedIDs (ids->str (:ids subject-specification))}
   :content nil})

(defn- selected-entities-subject-specification->xml
  [subject-specification]
  {:tag :subjectSpecification
   :attrs {:type "selectedEntities"
           :selectedIDs (ids->str (:ids subject-specification))}
   :content nil})

(defn subject-specification->xml
  [subject-specification]
  (condp = (:type subject-specification)
    "all" (all-subject-specification->xml subject-specification)
    "selected-types" (selected-types-subject-specification->xml subject-specification)
    "selected-entities" (selected-entities-subject-specification->xml subject-specification)
    nil nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; XML -> :area-specification ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- xml->all-area-specification
  [xml]
  {:type "all"})

(defn- xml->selected-zones-area-specification
  [xml]
  {:type "selected-zones" :ids (seq (id-set-from-xml xml :selectedIDs))})

(defn- xml->selected-maps-area-specification
  [xml]
  {:type "selected-maps" :ids (seq (id-set-from-xml xml :selectedIDs))})

(defn xml->area-specification
  [xml]
  (when xml
    (condp = (-> xml :attrs :type)
      "all" (xml->all-area-specification xml)
      "selectedZones" (xml->selected-zones-area-specification xml)
      "selectedMaps" (xml->selected-maps-area-specification xml)
      (raise :type :unknown-subject-specification-type))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; :area-specification -> XML ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- all-area-specification->xml
  [area-specification]
  {:tag :areaSpecification
   :attrs {:type "all"}
   :content nil})

(defn- selected-zones-area-specification->xml
  [area-specification]
  {:tag :areaSpecification
   :attrs {:type "selectedZones" :selectedIDs (ids->str (:ids area-specification))}
   :content nil})

(defn- selected-maps-area-specification->xml
  [area-specification]
  {:tag :areaSpecification
   :attrs {:type "selectedMaps" :selectedIDs (ids->str (:ids area-specification))}
   :content nil})

(defn area-specification->xml
  [area-specification]
  (condp = (:type area-specification)
    "all" (all-area-specification->xml area-specification)
    "selected-zones" (selected-zones-area-specification->xml area-specification)
    "selected-maps" (selected-maps-area-specification->xml area-specification)
    nil nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; XML -> :sensor-specification ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- sensor-validation
  [{:keys [transmit-period tries logging-period] :as sensor}]
  (let [tries-p           #(and (number? %) (<= 1 % 5))
        transmit-period-p #{"4 min" "5 min" "10 min"
                            "15 min" "30 min" "60 min"}
        logging-period-p  #{"Disabled"}]
    (if (and (tries-p tries)
          (transmit-period-p transmit-period)
          (logging-period-p logging-period))
      sensor
      (raise :type :invalid-sensor-specification))))

(defn- xml->sensor-specification
  [xml]
  (when xml
    (sensor-validation
      {:type "sensor"
       :transmit-period (-> xml :attrs :transmitPeriod)
       :tries           (-> xml :attrs :tries parse-number)
       :logging-period  (-> xml :attrs :loggingPeriod)})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; :sensor-specification -> XML ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn sensor-specification->xml
  [{:keys [transmit-period tries logging-period] :as sensor-spec}]
  (when sensor-spec
    {:tag :sensorSpecification
     :attrs {:transmitPeriod (str transmit-period)
             :tries          (str tries)
             :loggingPeriod  (str logging-period)}
     :content nil}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; XML -> :alarm-specification ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- alarm-validation
  [{:keys [high-enable high-value high-time low-enable low-value low-time
           value-unit time-unit hysteresis alarm-on-exit] :as alarm}]
  (let [alarm-p (fn [enable? value time]
                  (or (not enable?)
                    (and (not (nil? value)) (not (nil? time)))))
        value-unit-p #{"C" "F"}
        time-unit-p #{"sec" "min"}]
    (if (and (or high-enable low-enable)
          (alarm-p high-enable high-value high-time)
          (alarm-p low-enable low-value low-time)
          (value-unit-p value-unit)
          (time-unit-p time-unit))
      alarm
      (raise :type :invalid-alarm-specification))))

(defn- xml->alarm-specification
  [xml]
  (when xml
    (alarm-validation
      {:type "alarm"
       :high-enable   (-> xml :attrs :highEnable parse-boolean)
       :high-value    (-> xml :attrs :highValue parse-double)
       :high-time     (-> xml :attrs :highTime parse-number)
       :low-enable    (-> xml :attrs :lowEnable parse-boolean)
       :low-value     (-> xml :attrs :lowValue parse-double)
       :low-time      (-> xml :attrs :lowTime parse-number)
       :value-unit    (-> xml :attrs :valueUnit)
       :time-unit     (-> xml :attrs :timeUnit)
       :hysteresis    (-> xml :attrs :hysteresis parse-double)
       :alarm-on-exit (-> xml :attrs :alarmOnExit parse-boolean)})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; :alarm-specification -> XML ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn alarm-specification->xml
  [{:keys [high-enable high-value high-time low-enable low-value low-time
           value-unit time-unit hysteresis alarm-on-exit] :as alarm-spec}]
  (when alarm-spec
    {:tag :alarmSpecification
     :attrs {:highEnable  (str high-enable)
             :highValue   (str high-value)
             :highTime    (str high-time)
             :lowEnable   (str low-enable)
             :lowValue    (str low-value)
             :lowTime     (str low-time)
             :valueUnit   (str value-unit)
             :timeUnit    (str time-unit)
             :hysteresis  (str hysteresis)
             :alarmOnExit (str alarm-on-exit)}
     :content nil}))

;;;;;;;;;;;;;;;;;;;;;
;; XML -> Triggers ;;
;;;;;;;;;;;;;;;;;;;;;

(defn- string->buttons
  [s]
  (when s
    (let [buttons (re-split #"," s)]
      (when-not (every? model/valid-buttons buttons)
        (raise :type :unknown-button))
      buttons)))

(defn- xml->button-trigger
  [xml]
  {:type "button"
   :buttons (string->buttons (-> xml :attrs :buttons))
   :subject-specification (xml->subject-specification (get-first-child-element xml :subjectSpecification))
   :area-specification (xml->area-specification (get-first-child-element xml :areaSpecification))})

(defn- xml->area-trigger
  [xml]
  {:type "area"
   :subtype (-> xml :attrs :subtype)
   :subject-specification (xml->subject-specification (get-first-child-element xml :subjectSpecification))
   :area-specification (xml->area-specification (get-first-child-element xml :areaSpecification))})

(defn- xml->battery-trigger
  [xml]
  {:type "battery"
   :min-level (parse-number (-> xml :attrs :minLevel))
   :subject-specification (xml->subject-specification (get-first-child-element xml :subjectSpecification))})

(defn- xml->safety-switch-trigger
  [xml]
  {:type "safety-switch"
   :cancel-by-closing (parse-boolean (-> xml :attrs :cancelByClosing))
   :subject-specification (xml->subject-specification (get-first-child-element xml :subjectSpecification))
   :area-specification (xml->area-specification (get-first-child-element xml :areaSpecification))})

(defn- xml->motion-trigger
  [xml]
  {:type "motion"
   :subject-specification (xml->subject-specification (get-first-child-element xml :subjectSpecification))
   :area-specification (xml->area-specification (get-first-child-element xml :areaSpecification))})

(defn- xml->par-level-trigger
  [xml]
  {:type "par-level"
   :zone-id (-> xml :attrs :zoneId parse-id)
   :subject-specification (xml->subject-specification (get-first-child-element xml :subjectSpecification))
   :low-limit (or (-> xml :attrs :lowLimit parse-number) 0)
   :high-limit (or (-> xml :attrs :highLimit parse-number) Integer/MAX_VALUE)})

(defn- xml->temperature-trigger
  [xml]
  {:type "temperature"
   :subject-specification (xml->subject-specification (get-first-child-element xml :subjectSpecification))
   :sensor-specification (xml->sensor-specification (get-first-child-element xml :sensorSpecification))
   :alarm-specification (xml->alarm-specification (get-first-child-element xml :alarmSpecification))})

(defn xml->trigger
  [xml]
  (when xml
    (condp = (-> xml :attrs :type)
      "button" (xml->button-trigger xml)
      "area" (xml->area-trigger xml)
      "battery" (xml->battery-trigger xml)
      "safetySwitch" (xml->safety-switch-trigger xml)
      "motion" (xml->motion-trigger xml)
      "parLevel" (xml->par-level-trigger xml)
      "temperature" (xml->temperature-trigger xml)
      (raise :type :unknown-trigger-type))))

;;;;;;;;;;;;;;;;;;;;;
;; Triggers -> XML ;;
;;;;;;;;;;;;;;;;;;;;;

(defn- vec-or-nil
  [coll]
  (if (< 0 (count coll))
    (vec coll)
    nil))

(defn- create-specifications-content
  [trigger]
  (vec-or-nil (remove nil?
                [(subject-specification->xml (:subject-specification trigger))
                 (area-specification->xml (:area-specification trigger))
                 (sensor-specification->xml (:sensor-specification trigger))
                 (alarm-specification->xml (:alarm-specification trigger))])))

(defn- button-trigger->xml
  [trigger]
  {:tag :trigger
   :attrs {:type "button"
           :buttons (ids->str (:buttons trigger))}
   :content (create-specifications-content trigger)})

(defn- area-trigger->xml
  [trigger]
  {:tag :trigger
   :attrs {:type "area"
           :subtype (-> trigger :subtype)}
   :content (create-specifications-content trigger)})

(defn- battery-trigger->xml
  [trigger]
  {:tag :trigger
   :attrs {:type "battery"
           :minLevel (str (:min-level trigger))}
   :content (create-specifications-content trigger)})

(defn- safety-switch-trigger->xml
  [trigger]
  {:tag :trigger
   :attrs {:type "safetySwitch"
           :cancelByClosing (if (:cancel-by-closing trigger) "true" "false")}
   :content (create-specifications-content trigger)})

(defn- motion-trigger->xml
  [trigger]
  {:tag :trigger
   :attrs {:type "motion"}
   :content (create-specifications-content trigger)})

(defn- par-level-trigger->xml
  [trigger]
  {:tag :trigger
   :attrs {:type "parLevel"
           :zoneId (str (:zone-id trigger))
           :lowLimit (str (:low-limit trigger))
           :highLimit (str (:high-limit trigger))}
   :content (create-specifications-content trigger)})

(defn- temperature-trigger->xml
  [trigger]
  {:tag :trigger
   :attrs {:type "temperature"}
   :content (create-specifications-content trigger)})

(defn trigger->xml
  [trigger]
  (condp = (:type trigger)
    "button" (button-trigger->xml trigger)
    "area" (area-trigger->xml trigger)
    "battery" (battery-trigger->xml trigger)
    "safety-switch" (safety-switch-trigger->xml trigger)
    "motion" (motion-trigger->xml trigger)
    "par-level" (par-level-trigger->xml trigger)
    "temperature" (temperature-trigger->xml trigger)))

;;;;;;;;;;;;;;;;;;;
;; XML -> Action ;;
;;;;;;;;;;;;;;;;;;;

(defn xml->action
  [xml]
  (println "the type is  : " (-> xml :attrs :type))
  (when xml
    (let [type (-> xml :attrs :type)]
      (condp = type
        "composite" (entity/xml->entity composite-action xml)
        "tagMessage" (entity/xml->entity tag-message-action xml message-target-entities)
        "emerginMessage" (entity/xml->entity emergin-message-action xml)
        "notification" (entity/xml->entity notification-action xml user-target-entities)
        "alert" (entity/xml->entity alert-action xml user-target-entities)
        "email" (entity/xml->entity email-action xml)
        "httpget" (entity/xml->entity httpget-action xml)
        :none {:type :none}))))

;;;;;;;;;;;;;;;;;;;
;; Action -> XML ;;
;;;;;;;;;;;;;;;;;;;

(defn- action->xml
  [action]
  (entity/entity->xml composite-action action))

;;;;;;;;;;;;;;;;;;;;;;;
;; XML -> Event Rule ;;
;;;;;;;;;;;;;;;;;;;;;;;

(defn xml->event-rule
  [{{:keys [name description disabled]} :attrs :as xml}]
  (println "in there in xml->event-rule") 
  {:name name
   :description description
   :disabled? (parse-boolean disabled)
   :trigger (xml->trigger (get-first-child-element xml :trigger))
   :action (xml->action (get-first-child-element xml :action))})

;;;;;;;;;;;;;;;;;;;;;;;
;; Event Rule -> XML ;;
;;;;;;;;;;;;;;;;;;;;;;;

(defn event-rule->xml
  [event-rule]
  {:tag :eventRule
   :attrs {:id (str (:id event-rule))
           :name (:name event-rule)
           :description (:description event-rule)
           :version (str (:version event-rule))
           :active (str (:active? event-rule))
           :disabled (str (:disabled? event-rule))
           :knowledge-begin-time (str (:knowledge-begin-time event-rule))
           :knowledge-end-time (str (:knowledge-end-time event-rule))}
   :content (let [contents (remove nil?
                             [(trigger->xml (:trigger event-rule))
                              (action->xml (:action event-rule))])]
              (when (< 0 (count contents))
                (vec contents)))})
