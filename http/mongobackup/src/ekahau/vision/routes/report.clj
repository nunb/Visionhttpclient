(ns ekahau.vision.routes.report
  (:require
   [ekahau.vision.routes.building]
   [ekahau.vision.json.asset-spec :as json-asset-spec])
  (:use
    [clojure.contrib.logging :only [debug]]
    [clojure.contrib.trace :only [trace]]
    compojure.core
    [ekahau.reports :only [create-asset-counts-per-zone-over-time]]
    [ekahau.string :only [parse-id parse-number]]
    [ekahau.util :only [keys-recursively-to-clojure
                        keys-recursively-to-camel-case]]
    [ekahau.vision.database :as database]
    [ekahau.vision.location-history-report-service]
    [ekahau.vision.routes.helpers :only [create-no-content-response
                                         json-from-request-body
                                         parse-request-route-id
                                         to-json
                                         with-json-content-type]])
  (:use
   [ekahau.vision.location-history-report-service]))

(defn report-uri-by-id
  ([id]
     (report-uri-by-id id "json"))
  ([id presentation-format]
     (format "reports/%s.%s" id presentation-format)))

(defn report-location
  [request id]
  (format "http://%s/%s"
          (get-in request [:headers "host"])
          (report-uri-by-id id)))

(defn create-resource-created-response
  [request resource]
  {:status 201
   :body (to-json (keys-recursively-to-camel-case resource))
   :headers {"Location"
             (report-location request (:id resource))}})

(defn verify-selected-assets
  [report]
  (if-let [selected-assets (:selected-assets report)]
    (assoc report :selected-assets
           (json-asset-spec/types-to-clojure selected-assets))
    report))

(defn post-json-report
  [db request]
  (let [request-body (json-from-request-body request)
        report (database/assoc-new-entity-id!
                db :reports
                (-> request-body
                    (select-keys [:name
                                  :description
                                  :timeInterval
                                  :type
                                  :selectedRows
                                  :selectedAssets
                                  :selectedColumns
                                  :columnsRequest])
                    (keys-recursively-to-clojure)
                    (verify-selected-assets)
                    (assoc :status "in-progress"
                           :progress 0)))]
    (database/put-entity! db :reports report)
    (start-report! db report)
    (create-resource-created-response request report)))

(defn get-json-report
  [db request]
  (let [id (parse-request-route-id request)
        report (database/get-entity-by-id db :reports id)]
    (to-json (keys-recursively-to-camel-case report))))

(defn get-json-reports
  [db request]
  (to-json
   (map
    (fn [report]
      (-> report
          (select-keys [:id :name :time-interval])
          (keys-recursively-to-camel-case)))
    (database/get-entities db :reports))))

(defn delete-json-report
  [db request]
  (let [id (parse-request-route-id request)]
    (database/delete-entity! db :reports id)
    (to-json {:result "OK"})))

(defn valid-selection?
  [selection]
  (and (sequential? selection)
       (every? string? selection)))

(defn put-json-report-selected-items-by-key
  [db request k]
  (let [id (parse-request-route-id request)
        request-body (json-from-request-body request)]
    (if (valid-selection? request-body)
      (let [report (database/get-entity-by-id db :reports id)]
        (database/put-entity! db (assoc report
                                   k request-body))
        (to-json {:result "OK"}))
      {:status 400})))

(defn create-report-routes
  [db]
  (->
   (routes
    (POST "/reports.json" {:as request}
      (post-json-report db request))
    (GET "/reports.json" {:as request}
      (get-json-reports db request))
    (GET "/reports/:id.json" {:as request}
      (get-json-report db request))
    (PUT "/reports/:id/selectedRows.json" {:as request}
      (put-json-report-selected-items-by-key db request :selected-rows))
    (PUT "/reports/:id/selectedColumns.json" {:as request}
      (put-json-report-selected-items-by-key db request :selected-columns))
    (DELETE "/reports/:id.json" {:as request}
      (delete-json-report db request)))
   (with-json-content-type)))
