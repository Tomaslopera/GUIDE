CREATE TABLE categories(
	category_id INT PRIMARY KEY,
	category TEXT NOT NULL
);

CREATE TABLE book(
	book_id INT PRIMARY KEY,
	category_id INT NOT NULL REFERENCES Categories(category_id),
	title TEXT NOT NULL,
	upc TEXT UNIQUE,
	product_type TEXT NOT NULL,
	url TEXT UNIQUE,
	description TEXT NOT NULL
);

CREATE TABLE book_price (
  	book_id INT REFERENCES book(book_id),
	price REAL,
  	price_excl_tax REAL,
  	price_incl_tax REAL,
  	tax NUMERIC,
  	PRIMARY KEY (book_id)
);

CREATE TABLE book_availability (
  	book_id INT REFERENCES book(book_id),
  	disponibility BOOLEAN,
  	stock INT,
  	availability_info TEXT,
  	PRIMARY KEY (book_id)
);

CREATE TABLE book_review (
  	book_id INT REFERENCES book(book_id),
  	calification INT,
  	n_reviews INT,
  	PRIMARY KEY (book_id)
);


-- MODELO ESTRELLA --

CREATE TABLE dim_category (
	category_id INT PRIMARY KEY,
	category TEXT NOT NULL
);

CREATE TABLE dim_book (
	book_id INT PRIMARY KEY,
	title TEXT NOT NULL,
	upc TEXT UNIQUE,
	product_type TEXT NOT NULL,
	url TEXT UNIQUE,
	description TEXT NOT NULL
);

CREATE TABLE dim_date (
  date_key INT PRIMARY KEY,
  date DATE NOT NULL,
  year INT, 
  month INT, 
  day INT
);

CREATE TABLE fact_table (
	book_id INT NOT NULL REFERENCES dim_book(book_id),
  	category_id INT NOT NULL REFERENCES dim_category(category_id),
  	date_key INT NOT NULL REFERENCES dim_date(date_key),
  	price REAL,
  	price_excl_tax REAL,
  	price_incl_tax REAL,
  	tax REAL,
  	stock INT,
  	disponibility BOOLEAN,
  	calification INT,
  	n_reviews INT,
  	PRIMARY KEY (book_id) 
);

DROP TABLE fact_table


INSERT INTO dim_category (category_id, category)
SELECT c.category_id, c.category
FROM categories c

SELECT * FROM dim_category

INSERT INTO dim_book (book_id, title, upc, product_type, url, description)
SELECT b.book_id, b.title, b.upc, b.product_type, b.url, b.description 
FROM book b

SELECT * FROM dim_book

INSERT INTO dim_date (date_key, date, year, month, day)
VALUES (
  TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::INT,
  CURRENT_DATE,
  EXTRACT(YEAR  FROM CURRENT_DATE)::INT,
  EXTRACT(MONTH FROM CURRENT_DATE)::INT,
  EXTRACT(DAY   FROM CURRENT_DATE)::INT
)

SELECT * FROM dim_date

INSERT INTO fact_table (
  book_id, category_id, date_key,
  price, price_excl_tax, price_incl_tax, tax,
  stock, disponibility, calification, n_reviews
)
SELECT
  db.book_id,
  db.category_id,
  TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::INT AS date_key,
  bp.price, bp.price_excl_tax, bp.price_incl_tax, bp.tax,
  ba.stock, ba.disponibility,
  br.calification, br.n_reviews
FROM book db
LEFT JOIN book_price          bp ON bp.book_id = db.book_id
LEFT JOIN book_availability  ba ON ba.book_id = db.book_id
LEFT JOIN book_review        br ON br.book_id = db.book_id

SELECT * FROM fact_table

-- CONSULTAS --

-- ¿Cuántas categorías de libros se tienen?
SELECT COUNT(*) AS num_categorias FROM dim_category

-- ¿Cuántos libros hay por categoría hay?
SELECT dc.category , COUNT(*) AS libros_x_categoria
FROM dim_category dc
LEFT JOIN fact_table f ON f.category_id = dc.category_id
GROUP BY dc.category
ORDER BY libros_x_categoria DESC

-- ¿Cuál es el libro más caro?
SELECT b.title, ft.price
FROM fact_table ft 
INNER JOIN dim_book b ON ft.book_id = b.book_id
ORDER BY ft.price DESC LIMIT 1

-- ¿Hay algún libro que esté en dos categorías?
SELECT b.book_id, COUNT(DISTINCT ft.category_id) AS categorias_distintas
FROM dim_book b
JOIN fact_table ft
ON b.book_id = ft.book_id 
GROUP BY b.book_id
HAVING COUNT(DISTINCT ft.category_id) > 1;

-- ¿Cuál es el libro más barato por categoría?
WITH ranked AS(
	SELECT c.category, b.title, ft.price, DENSE_RANK() OVER (PARTITION BY category ORDER BY price ASC NULLS LAST) AS rnk
	FROM fact_table ft 
	INNER JOIN dim_book b ON ft.book_id = b.book_id
	INNER JOIN dim_category c ON ft.category_id = c.category_id	
)
SELECT category, title, price
FROM ranked 
WHERE rnk = 1


-- ¿Cuánto más caro o barato es cada libro con respecto al promedio de su categoría?
WITH base AS (
  	SELECT c.category, b.title, f.price
  	FROM fact_table f
  	JOIN dim_book b ON b.book_id = f.book_id
  	JOIN dim_category c ON c.category_id = f.category_id
),
stats AS (
  	SELECT category, AVG(price) AS avg_price
  	FROM base
  	GROUP BY category
)
SELECT
  	a.category,
  	a.title,
  	a.price,
  	s.avg_price,
  	(a.price - s.avg_price) AS delta_price
FROM base a
JOIN stats s USING (category)
ORDER BY a.category, delta_price DESC NULLS LAST, a.title;

-- Asumiendo que se venden todos los libros que están en stock en este momento
-- ¿Cuál es el libro que daría más ingresos por categoría?
WITH revenue AS (
  SELECT
    c.category,
    b.title,
    f.price,
    f.stock,
    (f.price * COALESCE(f.stock, 0))::numeric AS revenue
  FROM fact_table f
  JOIN dim_book b      ON b.book_id = f.book_id
  JOIN dim_category c  ON c.category_id = f.category_id
),
ranked AS (
  SELECT *,
         RANK() OVER (PARTITION BY category ORDER BY revenue DESC NULLS LAST) AS rnk
  FROM revenue
)
SELECT category, title, price, stock, revenue
FROM ranked
WHERE rnk = 1
ORDER BY category, revenue DESC NULLS LAST;








