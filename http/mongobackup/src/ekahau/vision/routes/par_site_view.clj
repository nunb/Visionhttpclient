(ns ekahau.vision.routes.par-site-view
  (:require
    [ekahau.vision.database :as database]
    [ekahau.vision.par-management :as pm]
    [ekahau.vision.par-history-service :as phs]
    [ekahau.vision.model.positions :as positions])
  (:use
    [ekahau.string :only [parse-int parse-long]]
    [clojure.contrib.logging :only [error]]
    [ekahau.vision.event-rule-service :only [get-event-rule-by-id]]
    compojure.core
    ekahau.vision.routes.helpers))

;; ## Par Rules

(defn create-par-rules-response
  [db request]
  (with-useful-bindings request {}
    (try
      (to-json (pm/get-active-par-management-rules db))
      (catch Exception e
        (error "Error fetching active par management rules" e)
        (create-bad-request-response request (.getMessage e) to-json)))))

;; ## Predicates

(defn create-selected-rule?-predicate
  [db request]
  (let [selected-rule-ids (:selected-par-rules
                           (get-user-by-request-session db request))]
    (if (and (get-in request [:params :showSelected]) selected-rule-ids)
     (let [s (set selected-rule-ids)]
       #(s (:id %)))
     (constantly true))))

(defn par-level-rule-zone-satisfies?
  [db pred rule]
  (when-let [zone (positions/get-zone-by-id db (pm/get-par-rule-zone-id rule))]
    (pred zone)))

(defn zone-in-building?
  [db building-id zone]
  (:building-id (positions/get-floor-from-map-id db (-> zone :area :map-id))))

(defn zone-on-map?
  [map-id zone]
  (= map-id (-> zone :area :map-id)))

;; ## Par Snapshots

(defn par-view-snapshot-for-matching-rules
  [db engine pred]
  (pm/flat-snapshot
   db
   @(:par-manager (:location-manager engine))
   (filter
    pred
    (pm/get-active-par-management-rules db))))

(defn create-par-snapshot-response-filtered-by-predicate
  [db engine request pred]
  (try
    (to-json (par-view-snapshot-for-matching-rules
              db
              engine
              pred))
    (catch Exception e
      (error "Error creating par snapshot for all rules" e)
      (create-bad-request-response request (.getMessage e) to-json))))

(defn create-par-snapshot-response
  [db engine request]
  (with-useful-bindings request {}
    (let [pred (create-selected-rule?-predicate db request)]
      (create-par-snapshot-response-filtered-by-predicate
       db engine request pred))))

(defn create-par-snapshot-response-for-some-par-rules
  [db engine request]
  (with-useful-bindings request
    {:json-body rule-ids}
    (let [rule-ids (set rule-ids)
          pred #(contains? rule-ids (:id %))]
      (create-par-snapshot-response-filtered-by-predicate
       db engine request pred))))

(defn create-par-snapshot-of-building-response
  [db engine request building-id]
  (let [pred (partial par-level-rule-zone-satisfies? db (partial zone-in-building? db building-id))]
    (create-par-snapshot-response-filtered-by-predicate
     db engine request pred)))

(defn create-par-snapshot-of-map-response
  [db engine request map-id]
  (let [pred (partial par-level-rule-zone-satisfies? db (partial zone-on-map? map-id))]
    (create-par-snapshot-response-filtered-by-predicate
     db engine request pred)))

;; ## Rule History

(defn create-par-rule-history-response
  [db engine request]
  (with-useful-bindings request
    {:id rule-id}
    (try
      (let [version (-> request :params :version parse-int)
            to (or (-> request :params :to parse-long)
                 (- (System/currentTimeMillis) (* 1000 60 60)))
            from (or (-> request :params :from parse-long)
                   (- to (* 1000 60 60 24 30)))
            par-rule (get-event-rule-by-id db rule-id version)]
        (to-json (phs/update-and-get-par-history! db (:par-historian (:location-manager engine))
                   [(pm/get-par-rule-zone-id par-rule)]
                   (pm/get-par-rule-asset-type-ids db par-rule)
                   from to)))
      (catch Exception e
        (error (str "Error getting par rule history for: " request) e)
        (create-bad-request-response request (.getMessage e) to-json)))))

;; ## Selected Rules

(defn put-selected-rules
  [db request]
  (with-useful-bindings request
    {:json-body rule-ids}
    (database/put-entity!
     db :users
     (assoc (get-user-by-request-session db request)
       :selected-par-rules rule-ids))))

(defn get-selected-rules
  [db request]
  (to-json
   (or (:selected-par-rules (get-user-by-request-session db request))
       (map :id (pm/get-active-par-management-rules db)))))

;; ## Routes

(defn create-par-site-view-routes
  [db engine]
  (-> (routes
        (GET "/parRules" {:as request}
          (create-par-rules-response db request))

        (GET "/parSnapshot" {:as request}
             (create-par-snapshot-response db engine request))
        
        (GET "/parSnapshot/buildings/:id" {{id :id} :params :as request}
             (create-par-snapshot-of-building-response db engine request id))
        
        (GET "/parSnapshot/maps/:id" {{id :id} :params :as request}
             (create-par-snapshot-of-map-response db engine request id))

        (POST "/parSnapshot" {:as request}
          (create-par-snapshot-response-for-some-par-rules db engine request))

        (GET "/parHistory/par-rule/:id/:version" {:as request}
          (create-par-rule-history-response db engine request))

        (PUT "/users/me/selectedParRules.json" {:as request}
          (put-selected-rules db request))

        (GET "/users/me/selectedParRules.json" {:as request}
          (get-selected-rules db request)))

    (with-json-content-type)))



