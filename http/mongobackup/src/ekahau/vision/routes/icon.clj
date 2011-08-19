(ns ekahau.vision.routes.icon
  (:use
    compojure.core
    [ring.util.response :only [file-response]]
    [ekahau.vision.routes.helpers :only [with-png-content-type with-xml-content-type]]
    [ekahau.xml :only [to-xml-str]])
  (:import
    [java.io File]))

(defn create-icon-routes
  []
  (routes
    (->
      (GET "/icons" {:as request}
        (let [files (seq (.. (File. "resources/icons") listFiles))
              image-files (filter #(.. ^File % getName toLowerCase (endsWith ".png")) files)]
          (to-xml-str [:icons (map #(vec [:icon {:name (.getName ^File %)}]) image-files)])))
      (with-xml-content-type))
    (->
      (GET "/icons/*" {:as request}
        (file-response (str "resources/icons/" (-> request :params :*))))
      (with-png-content-type))))
