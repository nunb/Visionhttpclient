(ns ekahau.vision.repl-utils)

(defn get-db
  []
  (:db @ekahau.vision.server/*server-agent*))

(defn init-user-ns
  []
  (in-ns 'user)
  (use '[ekahau.vision.repl-utils :only [get-db]])
  (require '[ekahau.vision.database :as database])
  (require '[ekahau.vision.location-history-report.simulation :as simulation])
  (require '[ekahau.vision.fake :as fake]))

(defn add-assets-to-todo-list
  [todo-list assets]
  (update-in
   todo-list [:items]
   (fn [items]
     (map
      (fn [asset]
        {:asset-id (:id asset) :checked-off? false})
      assets))))