(ns ekahau.vision.routes.event-rule
  (:require
    [ekahau.engine.cmd]
    [ekahau.vision.database :as database]
    [ekahau.engine.connection]
    [ekahau.vision.engine-service]
    [ekahau.vision.event-rule-service :as ers]
    [ekahau.vision.xml.event-rule :as xml])
  (:use
    [clojure.contrib.lazy-xml :only [emit]]
    [clojure.contrib.str-utils :only [str-join]]
    [clojure.contrib.logging :only [error]]
    compojure.core
    ekahau.vision.routes.helpers
    [ekahau.string :only [parse-id]]
    [ekahau.xml :only [to-xml-str]]))

(defn event-rule->xml
  [event-rule]
  (assoc-in (xml/event-rule->xml event-rule) [:attrs :uri] (str "eventRules/" (:id event-rule))))

(defn- create-get-event-rules-response
  [request db]
  (with-useful-bindings request
    {}
    (try
      (with-out-str
        (clojure.contrib.lazy-xml/emit
          {:tag :eventRules
           :attrs nil
           :content (->> (ers/get-event-rules db)
                      (map event-rule->xml)
                      (into []))}))
      (catch Exception e
        (error "Error while getting event rules" e)
        (create-bad-request-response request (.getMessage e) to-xml-str)))))

(defn- create-get-event-rule-response
  [request db]
  (with-useful-bindings request
    {:id event-rule-id}
    (try
      (when-let [event-rule (ers/get-event-rule-by-id db event-rule-id)]
        (with-out-str
          (clojure.contrib.lazy-xml/emit
            (event-rule->xml event-rule))))
      (catch Exception e
        (error "Error while getting event rule" e)
        (create-bad-request-response request (.getMessage e) to-xml-str)))))

(defn- create-get-event-rule-history-response
  [request db]
  (with-useful-bindings request
    {:id event-rule-id}
    (try
      (when-let [event-rule-versions (ers/get-event-rule-by-id-all-versions db event-rule-id) ]
        (with-out-str
          (clojure.contrib.lazy-xml/emit
            {:tag :eventRuleHistory
             :attrs nil
             :content (->> event-rule-versions
                        (map event-rule->xml)
                        (into []))})))
      (catch Exception e
        (error "Error while getting event rule history" e)
        (create-bad-request-response request (.getMessage e) to-xml-str)))))

(defn- disable-event-rule-response!
  [request db]
  (with-useful-bindings request
    {:id event-rule-id}
    (try
      (ers/disable-event-rule! db event-rule-id)
      (catch Exception e
        (error "Error while disabling rule" e)
        (create-bad-request-response request (.getMessage e) to-xml-str)))))

(defn- enable-event-rule-response!
  [request db]
  (with-useful-bindings request
    {:id event-rule-id}
    (try
      (ers/enable-event-rule! db event-rule-id)
      (catch Exception e
        (error "Error while enabling rule" e)
        (create-bad-request-response request (.getMessage e) to-xml-str)))))

(defn- assoc-model-id
  [db event-rule]
  (println "in assoc-model-id ") 
  (let [model-id (ekahau.vision.engine-service/get-active-model-id db)]
    (if (and model-id (:area-specification event-rule))
      (assoc-in event-rule [:area-specification :model-id] model-id)
      event-rule)))

;creates a new rule record and puts it into the db
(defn- new-event-rule-from-xml!
  [db xml]
  (println "printing here too") 
  (let [event-rule (->> (xml/xml->event-rule xml)
                     (assoc-model-id db)
                     (ers/assoc-event-rule-id! db))]
    (println "the new event-rule is " event-rule) 
    (ers/put-new-event-rule! db event-rule)
    event-rule))

(defn- create-new-event-rule-response!
  [request db]
  (with-useful-bindings request
    {:xml-body xml}
    (try
      {:status 200
       :body (to-xml-str [:eventRule
                          (select-keys
                            (new-event-rule-from-xml! db xml)
                            [:id])])}
      (catch Exception e
        (error "Error while inserting new event rule" e)
        (create-bad-request-response request (.getMessage e) to-xml-str)))))

(defn- update-event-rule-from-xml!
  [db event-rule-id xml]
  (ers/update-event-rule! db event-rule-id (xml/xml->event-rule xml)))

(defn- create-update-event-rule-response!
  [request db]
  (with-useful-bindings request
    {:xml-body xml
     :id event-rule-id}
    (try
      (update-event-rule-from-xml! db event-rule-id xml)
      (to-xml-str [:ok])
      (catch Exception e
        (error "Error while updating event rule" e)
        (create-bad-request-response request (.getMessage e) to-xml-str)))))

(defn create-event-rule-routes
  [db]
  (-> (routes
        (GET "/eventRules" {:as request}
          (create-get-event-rules-response request db))
        
        (GET "/eventRules/:id" {:as request}
          (create-get-event-rule-response request db))
        
        (GET "/eventRules/:id/history" {:as request}
          (create-get-event-rule-history-response request db))
        
        (POST "/eventRules" {:as request}
          (create-new-event-rule-response! request db))
        
        (POST "/eventRules/:id/update" {:as request}
          (create-update-event-rule-response! request db))
        
        (PUT "/eventRules/:id/disable" {:as request}
          (disable-event-rule-response! request db))
        
        (PUT "/eventRules/:id/enable" {:as request}
          (enable-event-rule-response! request db)))
    
    (with-xml-content-type)))
