(ns ekahau.vision.routes.debug
  (:use
    compojure.core
    clojure.pprint))

(defn- create-debug-response
  [request]
  (prn (:headers request))
  (prn (str "http://" (get-in request [:headers "host"]) "/bah" ))
  {:status 204})

(defn create-debug-routes
  [db engine]
  (routes
    (GET "/debug" {:as request}
      (with-out-str
        (println "----------------db---------------")
        (pprint db)
        (println "----------------engine---------------")
        (pprint engine)))
    (GET "/debug/db/:something" {:as request}
      (with-out-str
        (let [something-kw (-> request :params :something keyword)]
          (pprint (something-kw db)))))
    (POST "/debug" {:as request}
      (create-debug-response request))))

