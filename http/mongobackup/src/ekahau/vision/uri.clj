(ns ekahau.vision.uri)

(defn entity-uri
  [resource id]
  (str resource "/" id))

(defn asset-type-uri
  [id]
  (entity-uri "assetTypes" id))

(defn asset-spec-uri
  [id]
  (entity-uri "assetSpecs" id))

(defn map-uri
  [id]
  (entity-uri "maps" id))

(defn building-uri
  [id]
  (entity-uri "buildings" id))

(defn zone-uri
  [id]
  (entity-uri "zones" id))

(defn zone-grouping-uri
  [id]
  (entity-uri "zoneGroupings" id))

(defn zone-group-uri
  [grouping-id group-id]
  (str (zone-grouping-uri grouping-id) "/" group-id))

