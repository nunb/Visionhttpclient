(ns ekahau.excel
  (:use [clojure.contrib.java-utils :only [file]])
  (:import [jxl Cell CellType Sheet Workbook]
	   [java.io File]))

(defn- load-workbook
  [f]
  (Workbook/getWorkbook (file f)))

(defmulti get-cell-value {:private true} class)

(defmacro def-get-cell-value
  {:private true}
  [class method type get-format]
  (let [cell (with-meta (gensym) {:tag class})]
    (if get-format
      `(defmethod get-cell-value ~class
         [~cell]
         {:type ~type
          :value (~method ~cell)
          :format (~get-format ~cell)})
      `(defmethod get-cell-value ~class
         [~cell]
         {:type ~type
          :value (~method ~cell)}))))

(defmacro def-get-cell-values
  {:private true}
  [& values]
  (let [rows (map
               (fn [[class method type get-format]]
                 `(def-get-cell-value ~class ~method ~type ~get-format))
               (partition 4 values))]
    `(do ~@rows)))

(def-get-cell-values
  jxl.LabelCell   .getString ::string  nil
  jxl.BooleanCell .getValue  "database/boolean" nil
  jxl.NumberCell  .getValue  "database/number"  .getNumberFormat
  jxl.DateCell    .getDate   ::date    .getDateFormat)

(defmethod get-cell-value :default
  [^Cell cell]
  (if (= CellType/EMPTY (.getType cell))
    nil
    :error))

(defn- cells-to-values
  [cells]
  (map get-cell-value cells))

(defn- sheet-row->vector
  [^Sheet sheet row-index]
  (-> sheet
    (.getRow row-index)
    (cells-to-values)
    (concat (repeat nil))
    (->> (take (.getColumns sheet)))
    (vec)))

(defn- sheet->vectors
  [^Sheet sheet]
  (->>
    (for [row-index (range (.getRows sheet))]
      (sheet-row->vector sheet row-index))
    (remove (partial every? nil?))))

(defn- workbook-sheets->vectors
  [^Workbook workbook]
  (doall
    (->> (.getSheets workbook)
      (map (comp doall sheet->vectors)))))

(defn- apply-to-workbook-file
  "Applies the given function f with given args to a workbook opened from the
  given file. Workbook is closed when this function returns so any reference
  to the workbook or its parts might not work anymore. Beware of this
  aspecially with lazy sequences."
  [workbook-file f & args]
  (with-open [^Workbook workbook (load-workbook workbook-file)]
    (apply f workbook args)))

(defn load-sheets-as-vectors
  [f]
  (apply-to-workbook-file f workbook-sheets->vectors))

;; Operations on rows ;;

(defn- header-row
  [rows]
  (first rows))

(defn- value-rows
  [rows]
  (rest rows))

(defn- column-from-rows
  [rows col-index]
  (map #(get % col-index) rows))

(defn get-columns
  [rows]
  (apply (partial map vector) rows))

(defn- get-column-formats
  [columns]
  (map (comp distinct (partial map :format)) columns))
