(ns ekahau.vision.routes.zone-grouping
  (:require
    [ekahau.vision.database :as database])
  (:use
    [clojure.xml :only [parse]]
    [clojure.contrib.condition :only [handler-case raise]]
    compojure.core
    ekahau.vision.routes.helpers
    [ekahau.vision.xml.zone-grouping]
    [ekahau.xml :only [get-elements-of-type to-xml-str]]
    [ekahau.util :only [convert-keys-to-camel-case]]))

(defn valid-zone-grouping?
  [g]
  (every? #(= 1 (count (val %)))
          (group-by identity (mapcat :zone-ids (:groups g)))))

(defn- new-zone-grouping-from-xml!
  [db xml]
  (dosync
   (let [new-grouping (database/assoc-new-entity-id!
                       db :zone-groupings (xml->zone-grouping xml))]
      (when-not (valid-zone-grouping? new-grouping)
        (raise :type :invalid-zone-grouping))
      (database/put-entity! db :zone-groupings new-grouping)
      new-grouping)))

(defn- inc-version-or-fail
  [v1 v2]
  (if (nil? v1)
    0
    (do
      (when-not (or (nil? v1) (= v1 v2))
        (raise :type :invalid-version))
      (inc v1))))

(defn- update-zone-grouping-from-xml!
  [db id xml]
  (dosync
    (let [old-grouping (database/get-entity-by-id db :zone-groupings id)
          updated-grouping (-> (xml->zone-grouping xml)
                             (assoc :id id)
                             (update-in [:version]
                                        inc-version-or-fail
                                        (:version old-grouping)))]
      (comment
        (when-not (valid-zone-grouping? updated-grouping)
          (raise :type :invalid-zone-grouping)))
      (database/put-entity! db :zone-groupings updated-grouping)
      (assoc updated-grouping
        ::created (nil? old-grouping)))))

(defn get-zone-groupings-json
  [db request]
  (to-json
   (map
    (fn [{:keys [id name groups]}]
      {:id id
       :name name
       :groups (map
                convert-keys-to-camel-case
                groups)})
    (database/get-entities db :zone-groupings))))

(defn create-zone-grouping-routes
  [db]
  (routes
   (->
    (routes
     (GET "/zoneGroupings" {:as request}
       (to-xml-str
        (zone-groupings->xml (database/get-entities db :zone-groupings))))
     (GET "/zoneGroupings/:id" {:as request}
       (if-let [zone-grouping (database/get-entity-by-id
                               db
                               :zone-groupings
                               (parse-request-route-id request))]
         (to-xml-str (zone-grouping->xml zone-grouping))
         (create-resource-not-found-response request)))
     (POST "/zoneGroupings" {:as request}
       (let [body (parse (:body request))]
         (handler-case :type
           (let [new-grouping (new-zone-grouping-from-xml! db body)]
             {:status 201
              :body (to-xml-str [:zoneGrouping
                                 (select-keys new-grouping [:id])])})
           (handle :invalid-zone-grouping
             {:status 400
              :body (to-xml-str [:error])}))))
     (PUT "/zoneGroupings/:id" {:as request}
       (let [id (parse-request-route-id request)]
         (let [body (parse (:body request))]
           (handler-case :type
             (let [updated-grouping (update-zone-grouping-from-xml! db
                                                                    id
                                                                    body)]
               {:status (if (::created updated-grouping) 201 200)
                :body (to-xml-str (zone-grouping->xml updated-grouping))})
             (handle :invalid-zone-grouping
               {:status 400
                :body (to-xml-str [:error])})
             (handle :invalid-version
               {:status 409
                :body (to-xml-str [:error])})))))
     (DELETE "/zoneGroupings/:id" {:as request}
       (let [id (parse-request-route-id request)]
         (dosync (database/delete-entity! db :zone-groupings id))
         (to-xml-str [:ok]))))
    (with-xml-content-type))
   (->
    (routes
     (GET "/zoneGroupings.json" {:as request}
       (get-zone-groupings-json db request)))
    (with-json-content-type))))
