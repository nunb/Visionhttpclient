(ns ekahau.vision.routes.address
  (:use
    compojure.core
    [ekahau.vision.routes.helpers :only [with-xml-content-type]]
    [ekahau.vision.database :as database]
    [clojure.contrib.str-utils :only (re-split)]
    [ekahau.xml :only [to-xml-str]]))

(defn parse-id-path
  [s]
  (vec (map #(Long/parseLong (apply str %)) (remove empty? (re-split #"/" s)))))

(defn view-address
  [maps path]
  (let [path-ids (parse-id-path path)]
    (vec
      (concat
        [:address]
        (if (seq path-ids)
          [{:selected path}]
          nil)
        (if (seq maps)
          [[:level (map #(vec [:element (select-keys % [:id :name])]) (sort-by :name maps))]]
          nil)))))

(defn create-address-routes
  [db]
  (->
    (GET "/view/address*" {:as request}
      (to-xml-str (view-address (database/get-entities db :maps) (-> request :params :*))))
    (with-xml-content-type)))
