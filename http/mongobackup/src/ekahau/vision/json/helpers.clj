(ns ekahau.vision.json.helpers
  (:use
   [ekahau.util
    :only [keys-recursively-to-camel-case]]))

(defn prepare-for-json
  [m ks]
  (-> m
      (select-keys ks)
      (keys-recursively-to-camel-case)))

