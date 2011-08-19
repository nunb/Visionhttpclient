(ns ekahau.vision.xml.tag
  (:use
    [ekahau.string :only [parse-id]]
    [ekahau.util :only [assoc-if]]))

(defn- select-tag-icon
  "Selects a tag icon based on tag type"
  [type]
  (get {"t301b" "t301b.png", "t301a" "t301a.png"} type))

(defn- create-tag-response
  [properties asset-lookup-fn]
  (let [asset-id (parse-id (:assetid properties))
        default-keys (select-keys properties [:tagid :name :mac :serialnumber])]
    (assoc-if
      (if asset-id
        (if-let [asset (asset-lookup-fn asset-id)]
          (assoc default-keys :assetId (:id asset))
          (assoc default-keys :hasAssetInEngine true))
        (assoc default-keys :hasAssetInEngine false))
      (complement nil?)
      :icon (select-tag-icon (:type properties)))))

(defn create-tag-element
  [{properties :properties} asset-lookup-fn]
  [:tag (create-tag-response properties asset-lookup-fn)])
