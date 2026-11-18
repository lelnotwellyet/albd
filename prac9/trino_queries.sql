-- 1. See both sources
SHOW CATALOGS;

-- 2. Read from source 1 (MySQL)
SELECT * FROM mysql.university.students;

-- 3. Create and fill a table in source 2 (memory)
CREATE TABLE memory.default.marks (student_id INT, subject VARCHAR, marks INT);

INSERT INTO memory.default.marks VALUES
(101, 'Big Data', 88),
(102, 'Big Data', 76),
(103, 'Big Data', 91),
(104, 'Big Data', 69);

-- 4. THE MAIN QUERY: join both sources in one SQL statement
SELECT s.student_name, s.department, m.subject, m.marks
FROM mysql.university.students AS s
JOIN memory.default.marks AS m
  ON s.student_id = m.student_id
ORDER BY m.marks DESC;
