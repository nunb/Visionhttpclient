(ns ekahau.vision.routes.system
  (:use
    compojure.core
    ekahau.vision.routes.helpers))

(defn create-system-routes
  []
  (-> (routes 
        (GET "/system/status" {:as request}
          (to-json
            (eval '(ekahau.vision.server/get-status))))) ; Avoiding circular dependency
    (with-json-content-type)))
