(ns ekahau.vision.routes.tag
  (:require
   [clojure.java.io :as io]
   [ekahau.engine.connection]
   [ekahau.engine.cmd])
  (:use
   clojure.set
   [clojure.xml :only [parse]]
   [clojure.contrib.json
    :only [read-json]]
   compojure.core
   [ekahau.vision.database :as database]
   [ekahau.vision.xml.asset :only [create-asset-element]]
   [ekahau.vision.xml.tag :only [create-tag-element]]
   [ekahau.vision.model.asset :only (has-engine-asset-id?
                                     get-all-asset-types
                                     create-get-property-type-by-id-fn)]
   [ekahau.vision.routes.helpers :only [parse-request-route-id

                                        to-json
                                        with-json-content-type
                                        with-xml-content-type
                                        create-page-not-found-response]]
   [ekahau.string :only [parse-id]]
   [ekahau.xml :only [to-xml-str]]))

;; Asset Look-up for Tags ;;

(defn- create-asset-id-index
  [db]
  (let [assets (database/get-entities db :assets)]
    (index (set (filter has-engine-asset-id? assets)) [:engine-asset-id])))

(defn- create-asset-lookup-fn
  [db]
  (let [asset-id-index (create-asset-id-index db)]
    (fn [asset-id]
      (first (get asset-id-index {:engine-asset-id asset-id})))))

(defn- create-tag-elements
  [db tags]
  (let [asset-lookup-fn (create-asset-lookup-fn db)]
    (map #(create-tag-element % asset-lookup-fn) tags)))

(defn- find-asset-by-engine-asset-id
  [db engine-asset-id]
  (first (select #(-> % :engine-asset-id (= engine-asset-id)) (set (database/get-entities db :assets)))))

(defn- get-tags
  [mac-address serial-number text]
  (ekahau.engine.cmd/epe-get-tags
    (reduce
      (fn [m [k v]] (if v (assoc m k v) m))
      {:fields "all" :pagesize 20 :from 0}
      [[:mac mac-address] [:serial serial-number] [:text text]])))

(defn- create-asset-element-by-id
  "This is copied from ekahau.vision.routes.asset and should be unified when possible."
  [db id]
  (let [asset (database/get-entity-by-id db id)
        asset-types (get-all-asset-types db (:asset-type-id asset))
        property-id-to-property-path-map (create-get-property-type-by-id-fn asset-types)]
    (create-asset-element asset property-id-to-property-path-map)))

(defn post-tag-search-json
  [db body]
  (to-json
   (let [f (create-asset-lookup-fn db)]
     (map
      (fn [{tag :properties}]
        (let [asset-id (parse-id (:assetid tag))
              asset (f asset-id)]
          (clojure.contrib.trace/trace
           (into (select-keys tag [:tagid :name :mac :serialnumber])
                 (map vec
                  (filter
                   (fn [[_ value]] value)
                   (partition
                    2 
                    [:assetId (:id asset)
                     :hasAssetInEngine (and asset-id (not asset))
                     :icon (case (:type tag)
                                 "t301b" "t301b.png"
                                 "t301a" "t301a.png"
                                 nil)])))))))
      (get-tags (:macAddress body) (:serialNumber body) (:text body))))))

(defn create-tag-routes
  [db]
  (routes
   (->
    (routes
     (POST "/tags/search.json" {request-body :body}
           (let [body (read-json (io/reader request-body))]
             (post-tag-search-json db body))))
    (with-json-content-type))
   (->
    (routes
     (GET "/tags/:id/asset" {:as request}
          (or
           (let [id (parse-request-route-id request)]
             (when-first [tag-emsg (ekahau.engine.cmd/epe-get-tags {:fields "all" :tagid id})]
               (when-let [engine-asset-id (parse-id (-> tag-emsg :properties :assetid))]
                 (when-let [asset (find-asset-by-engine-asset-id db engine-asset-id)]
                   (to-xml-str (create-asset-element-by-id db (:id asset)))))))
           (create-page-not-found-response)))
     
     (GET "/tags/:id" {:as request}
          (let [id (parse-request-route-id request)]
            (or
             (when-first [tag-emsg (ekahau.engine.cmd/epe-get-tags {:fields "all" :tagid id})]
               (to-xml-str (create-tag-element tag-emsg #(find-asset-by-engine-asset-id db %))))
             (create-page-not-found-response))))
     
     (GET "/tags" {:as request}
          (let [tags (ekahau.engine.cmd/epe-get-tags)]
            (to-xml-str [:tags (create-tag-elements db tags)])))
     
     (POST "/tags/search" {:as request}
           (let [body (parse (:body request))
                 mac-address (-> body :attrs :macAddress)
                 serial-number (-> body :attrs :serialNumber)
                 text (-> body :attrs :text)]
             (to-xml-str [:tags (create-tag-elements db (get-tags mac-address serial-number text))]))))
    (with-xml-content-type))))
