(ns ekahau.vision.xml.map)

(defn- assoc-selected-true
  [m id]
  (if (= id (:id m))
    (assoc m :selected true)
    m))

(defn map-xml
  ([model-map]
    (map-xml model-map nil))
  ([model-map selected-map-id]
    [:map (-> model-map
            (select-keys [:id :name])
            (assoc :scale (-> model-map :scale :value))
            (assoc-selected-true selected-map-id))]))
