CREATE TABLE university.students (
    student_id INT PRIMARY KEY,
    student_name VARCHAR(100),
    department VARCHAR(100)
);

INSERT INTO university.students VALUES
(101, 'Aarav', 'Data Science'),
(102, 'Diya', 'Computer Science'),
(103, 'Rohan', 'Data Science'),
(104, 'Meera', 'Computer Science');

CREATE USER IF NOT EXISTS 'trino_user'@'%' IDENTIFIED BY 'trino123';
GRANT SELECT ON university.* TO 'trino_user'@'%';
