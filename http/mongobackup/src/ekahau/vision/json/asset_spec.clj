(ns ekahau.vision.json.asset-spec
  (:use
   [ekahau.util :only [update-values
                       camel-to-clojure
                       keys-recursively-to-clojure
                       keys-recursively-to-camel-case]]))

(defn parse-json-asset-property-value-spec
  [json-spec]
  {:type "asset-property-value"
   :key (:key json-spec)
   :value (:value json-spec)})

(defn type-values-to-clojure-case
  [m]
  (let [f (fn [[k v]] (if (= :type k) [k (camel-to-clojure v)] [k v]))]
    ;; only apply to maps
    (clojure.walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) m)))

(defn types-to-clojure
  [spec]
  (type-values-to-clojure-case spec))

(defn parse-json-asset-spec
  [json-spec]
  (-> json-spec
      (keys-recursively-to-clojure)
      (types-to-clojure)))

(defn assoc-id
  [result id]
  (if id
    (assoc result :id id)
    result))

(defn asset-spec-to-json
  [spec]
  (->
    (condp = (:type spec)
      "asset-property-value" {:type "assetPropertyValue"
                              :key (:key spec)
                              :value (:value spec)}
      "asset-type"           {:type "assetType"
                              :assetTypeId (:asset-type-id spec)}
      "map"                  {:type "map"
                              :mapId (:map-id spec)}
      "building"             {:type "building"
                              :buildingId (:building-id spec)}
      "true"                 {:type "true"}
      "false"                {:type "false"}
      "not"                  {:type "not"
                              :spec (asset-spec-to-json (:spec spec))}
      "and"                  {:type "and"
                              :specs (map asset-spec-to-json (:specs spec))}
      "or"                   {:type "or"
                              :specs (map asset-spec-to-json (:specs spec))}
      "asset-set"            {:type "assetSet"
                              :assetIds (vec (:asset-ids spec))}
      "asset-spec"           {:type "assetSpec"
                              :assetSpecId (:asset-spec-id spec)}
      "named"                {:type "named"
                              :name (:name spec)
                              :spec (asset-spec-to-json (:spec spec))}
      "zone"                 {:type "zone"
                              :zoneID (:zone-id spec)}
      "zone-group"           {:type "zoneGroup"
                              :zoneGroupingId (:zone-grouping-id spec)
                              :zoneGroupId (:zone-group-id spec)}
      "to-do-list"           {:type "toDoList"
                              :toDoListId (:to-do-list-id spec)}

       (throw (IllegalArgumentException. (str "Unknown spec type: "
                                              (:type spec)))))
   (assoc-id (:id spec))))
