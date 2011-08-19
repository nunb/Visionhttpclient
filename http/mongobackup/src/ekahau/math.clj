(ns ekahau.math)

; START Intervals

(defn create-interval-including
  [& values]
  [(apply min values) (apply max values)])

(defn- interval-includes?
  [[start end] value]
  (<= start value end))

(defn intervals-intersect?
  [[a-start a-end :as a] [b-start b-end :as b]]
  (boolean (or
    (some (partial interval-includes? a) b)
    (some (partial interval-includes? b) a)
  )))

; END Intervals

(defn vector-plus
  [v1 v2]
  (vec (map + v1 v2)))

(defn vector-negate
  [v]
  (vec (map (partial - 0) v)))

(defn vector-minus
  [v1 v2]
  (vec (map - v1 v2)))

(defn- vector-length-square
  [v]
  (apply + (map * v v)))

(defn vector-length
  [v]
  (Math/sqrt (vector-length-square v)))

(defn vector-multiply
  [v s]
  (vec (map (partial * s) v)))

(defn vector-to-unit
  [v]
  (vector-multiply v (/ (vector-length v))))

(defn vector-to-length
  [v l]
  (vector-multiply (vector-to-unit v) l))

(defn vector-distance
  [p1 p2]
  (vector-length (vec (map - p2 p1))))

(defn- create-3d-vector
  [v]
  (take 3 (concat v (repeat 0))))

(defn- vector-cross-product
  [a b]
  (let [[a1 a2 a3] (create-3d-vector a)
        [b1 b2 b3] (create-3d-vector b)]
    [(- (* a2 b3) (* a3 b2)), (- (* a3 b1) (* a1 b3)), (- (* a1 b2) (* a2 b1))]))

(defn vectors-coincident?
  [[a1 a2] [b1 b2]]
  (= 0.0 (- (* a1 b2) (* a2 b1))))

(defn segment-slope
  [[[x1 y1] [x2 y2]]]
  (/ (- y2 y1) (- x2 x1)))

(defn segment-y-at-x
  [[[x1 y1] [x2 y2] :as segment] x]
  (+ y1 (* (segment-slope segment) (- x x1))))

(defn- test-ray-from-point-intersects-polygon-segment?
  [[x y] [[x1 y1] [x2 y2] :as segment]]
  (and
    (<= (min x1 x2) x)
    (< x (max x1 x2))
    (< y (segment-y-at-x segment x))))

(defn polygon-segments
  [polygon]
  (take (count polygon) (partition 2 1 (cycle polygon))))

(defn polygon-includes?
  [polygon point]
  (->> polygon
       (polygon-segments)
       (filter (partial test-ray-from-point-intersects-polygon-segment? point))
       (count)
       (odd?)))

(defstruct rectangle :corner1 :corner2)

(defn get-bounding-box
  [points]
  (let [x-seq (map first points)
        y-seq (map second points)]
    (struct rectangle [(apply min x-seq) (apply min y-seq)] [(apply max x-seq) (apply max y-seq)])))

(def epsilon 0.0001)

(defn- very-small?
  [num]
  (< (Math/abs (double num)) epsilon))

(defn- get-line-segment-intersection-imp
  [[[ax1 ay1] [ax2 ay2] :as a] [[bx1 by1] [bx2 by2] :as b] result-fn]
  (let [denominator (- (* (- by2 by1) (- ax2 ax1)) (* (- bx2 bx1) (- ay2 ay1)))
        a-numerator (- (* (- bx2 bx1) (- ay1 by1)) (* (- by2 by1) (- ax1 bx1)))
        b-numerator (- (* (- ax2 ax1) (- ay1 by1)) (* (- ay2 ay1) (- ax1 bx1)))]
    (if-not (very-small? denominator)
      (let [ua (/ a-numerator denominator)
            ub (/ b-numerator denominator)]
        (if (and (<= 0 ua 1) (<= 0 ub 1))
          (result-fn a ua)))
      (if (let [[a1 a2] a
                [b1 b2] b]
            (and
              (vectors-coincident? (vector-minus a2 a1) (vector-minus b2 b1))
              (intervals-intersect? (create-interval-including ax1 ax2) (create-interval-including bx1 bx2))
              (intervals-intersect? (create-interval-including ay1 ay2) (create-interval-including by1 by2))))
        :coincident
        nil))))

(defn get-line-segment-intersection
  "Returns an intersection point of two line segments if such exists.
  Line segment is expected to be a vector of points: [[x1 y1] [x2 y2]]
  Returns: [x y] of intersection point
        or nil if line segments do not intersect
        or :coincident if linesegments are coincident"
  [a b]
  (get-line-segment-intersection-imp a b
    (fn [[[x1 y1] [x2 y2]] ratio]
      [(+ x1 (* ratio (- x2 x1))) (+ y1 (* ratio (- y2 y1)))])))

(defn get-line-segment-intersection-ratio
  [a b]
  (get-line-segment-intersection-imp a b
    (fn [a ratio]
      ratio)))

(defn- get-line-segment-intersection-point-and-ratio
  [a b]
  (get-line-segment-intersection-imp a b
    (fn [[[x1 y1] [x2 y2]] ratio]
      {:point [(+ x1 (* ratio (- x2 x1))) (+ y1 (* ratio (- y2 y1)))] :ratio ratio})))

(defn- vector-from-to
  [a b]
  (vector-minus b a))

(defn- get-point-distance-square
  [p1 p2]
  (vector-length-square (vector-from-to p1 p2)))

(defn get-points-distance
  [p1 p2]
  (Math/sqrt (get-point-distance-square p1 p2)))

(defn get-polygon-line-segment-intersections
  [polygon line-segment]
  (sort-by
    (partial get-point-distance-square (first line-segment))
    (distinct (remove #(or (nil? %) (= :coincident %))
      (map #(get-line-segment-intersection %1 %2) (polygon-segments polygon) (repeat line-segment))))))

(defn get-polygon-line-segment-intersections-as-ratios
  [polygon line-segment]
  (sort (distinct (remove nil? (map #(get-line-segment-intersection-ratio %2 %1) (polygon-segments polygon) (repeat line-segment))))))

(defn- perp-vector
  [[x y]]
  (vector-negate [y (- x)]))

(defn get-polygon-area
  "Returns the area of polygon. A counter clockwise polygon will have a positive area and
  a clockwise polygon negative area."
  [polygon]
  (let [func (fn [sum [[x1 y1] [x2 y2]]] (+ sum (- (* x1 y2) (* x2 y1))))]
    (* 1/2 (reduce func 0 (polygon-segments polygon)))))

(defn- get-polygon-direction
  [polygon]
  (let [area (get-polygon-area polygon)]
    (cond
      (< area 0) :clockwise
      (> area 0) :counter-clockwise)))

(defn get-point-side
  [[p1 p2] p3]
  (let [[xa ya :as a] (vector-from-to p1 p2)]
    (if-not (= 0.0 xa ya)
      (let [[xb yb :as b] (vector-from-to p1 p3)
            [xa_ ya_] (perp-vector a)
             t (/ (- (* xa yb)  (* ya xb))
                  (- (* xa ya_) (* xa_ ya)))]
        (cond
          (< t 0)  :negative
          (== t 0) :zero
          (< 0 t)  :positive))
      :none)))

(defn- get-polygon-side-mapping
  [polygon]
  (condp = (get-polygon-direction polygon)
    :counter-clockwise {:negative :enter, :positive :exit, :zero :passing}
    :clockwise         {:negative :exit, :positive :enter, :zero :passing}))

(defn- get-line-segments-intersection-event
  [a b sides]
  {:segment b :intersection (get-line-segment-intersection-point-and-ratio a b) :crossing-type (get sides (get-point-side b (first a)))})

(defn- get-intersection-events
  [polygon segment sides]
  (map #(get-line-segments-intersection-event segment % sides) (polygon-segments polygon)))

(defn get-polygon-line-segment-intersection-events
  [polygon line-segment]
  (sort-by :ratio
    (map
      #(let [{{ratio :ratio} :intersection side :crossing-type} %] {:ratio ratio :crossing-type side})
      (remove
        (comp (partial contains? #{nil :coincident}) :intersection)
        (get-intersection-events polygon line-segment (get-polygon-side-mapping polygon))))))

(defn interpolate
  [[start end] ratio]
  (+ start (* ratio (- end start))))

(defn get-max-min-avg
  [coll]
  (when-let [n (first coll)]
    (let [[max min sum]
          (reduce (fn [[ma mi s] n]
                    [(max ma n) (min mi n) (+ s n)])
            [n n n]
            (rest coll))]
      [max min (/ sum (count coll))])))
