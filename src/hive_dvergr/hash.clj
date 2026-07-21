(ns hive-dvergr.hash
  "Deterministic content addressing for EDN evidence."
  (:import [java.nio.charset StandardCharsets]
           [java.security MessageDigest]))

(declare canonical-form)

(defn- canonical-map [m]
  [:map
   (->> m
        (map (fn [[k v]] [(canonical-form k) (canonical-form v)]))
        (sort-by pr-str)
        vec)])

(defn canonical-form
  "Convert EDN to a stable, type-marked tree before hashing."
  [x]
  (cond
    (map? x)        (canonical-map x)
    (set? x)        [:set (->> x (map canonical-form) (sort-by pr-str) vec)]
    (vector? x)     [:vector (mapv canonical-form x)]
    (list? x)       [:list (mapv canonical-form x)]
    (sequential? x) [:seq (mapv canonical-form x)]
    :else           x))

(defn sha256
  "Return a `sha256:<hex>` content address for an EDN value."
  [value]
  (let [digest (.digest (MessageDigest/getInstance "SHA-256")
                        (.getBytes (pr-str (canonical-form value))
                                   StandardCharsets/UTF_8))]
    (str "sha256:" (apply str (map #(format "%02x" (bit-and % 0xff)) digest)))))
