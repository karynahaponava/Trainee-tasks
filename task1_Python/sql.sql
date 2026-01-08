USE task1;
/*
CREATE TABLE rooms (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE students (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    birthday DATE NOT NULL,
    sex ENUM('M', 'F') NOT NULL,
    room_id INT,
    FOREIGN KEY (room_id) REFERENCES rooms(id)
); 
*/

/*Список комнат и количество студентов в каждой из них*/
SELECT rooms.name, COUNT(students.id) AS students_count
FROM rooms LEFT JOIN students ON rooms.id = students.room_id
GROUP BY rooms.name;

/*5 комнат с самым маленьким средн возрастом студентов*/
select rooms.id, rooms.name, AVG(TIMESTAMPDIFF(YEAR, birthday, CURDATE())) AS avg_age
from rooms join students on students.room_id = rooms.id
group by rooms.name, rooms.id
order by avg_age ASC
limit 5;

/*5 комнат с самой большой разницей в возрасте студентов*/
select rooms.name, (max(TIMESTAMPDIFF(YEAR, birthday, CURDATE())) - min(TIMESTAMPDIFF(YEAR, birthday, CURDATE()))) as diff
from rooms join students on students.room_id = rooms.id
group by rooms.name
order by diff DESC
limit 5;

/*Список комнат, где проживают студенты разного пола*/
SELECT rooms.id, rooms.name
FROM rooms join students on students.room_id = rooms.id
GROUP BY rooms.id, rooms.name
HAVING COUNT(DISTINCT students.sex) > 1;
