(ns ekahau.vision.xml.asset-type
  (:require
    [ekahau.vision.database :as database])
  (:use
    [ekahau.util :only [convert-keys-to-camel-case dissoc-if]]
    [ekahau.vision.routes.helpers :only [str-id-path parse-id-string]]))

(defn- property-groups-xml
  [property-groups]
  (map
    (fn [property-group]
      [:propertyGroup (convert-keys-to-camel-case (select-keys property-group [:id :asset-type-id :name]))
       (map
         (fn [properties] [:property (update-in properties [:type] database/db-type-to-name)])
         (:properties property-group))
       ])
    property-groups))

(defn- assoc-selected-true
  [m id]
  (if (= id (:id m))
    (assoc m :selected true)
    m))

(defn asset-type-xml
  ([asset-type property-groups]
    (asset-type-xml asset-type property-groups nil))
  ([asset-type property-groups selected-asset-type-id]
    (into
      [:assetType (-> asset-type
                    (select-keys [:id :name :icon :description :version :parent-type-id :level])
                    (dissoc-if nil? :version :parent-type-id :level :description)
                    (assoc-selected-true selected-asset-type-id)
                    (convert-keys-to-camel-case))]
      (property-groups-xml property-groups))))

(defn asset-types-xml
  ([types]
    (asset-types-xml types nil))
  ([types selected-asset-type-id]
    [:assetTypes (map #(asset-type-xml % nil selected-asset-type-id) types)]))

(defn card->xml
  [card]
  (-> [:card {:titleKey (str-id-path (:title-key card))}]
    (into (map (fn [subtitle-key] [:subtitle {:key (str-id-path subtitle-key)}]) (:subtitle-keys card)))
    (into (map (fn [row] [:row (map (fn [field] [:field {:key (str-id-path field)}]) row)]) (:field-key-rows card)))))


(defn xml->card
  [xml]
  (let [title-key (-> xml :attrs :titleKey parse-id-string)
        subtitle-keys (-> xml :content
                        (->> (filter #(= :subtitle (:tag %)))
                          (map #(-> % :attrs :key parse-id-string))
                          (vec)))
        rows (-> xml :content
               (->> (filter #(= :row (:tag %)))
                 (map (fn [row-element]
                        (-> row-element :content
                          (->> (filter #(= :field (:tag %)))
                            (map #(-> % :attrs :key parse-id-string))
                            (vec)))))
                 (vec)))]
    {:title-key title-key
     :subtitle-keys subtitle-keys
     :field-key-rows rows}))
