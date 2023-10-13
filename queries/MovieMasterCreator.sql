CREATE TABLE LetterboxdVImdb.dbo.MovieMaster (
    id INT PRIMARY KEY IDENTITY(1,1),
	masterMovieId NVARCHAR(255),
    title NVARCHAR(255),
);

INSERT INTO LetterboxdVImdb.dbo.MovieMaster
	(masterMovieID, title)

SELECT 
	masterMovieId,
	title

FROM 
	LetterboxdVImdb.dbo.LBLetterboxdTop250

select*
from LetterboxdVImdb.dbo.MovieMaster

WITH CTE AS (
    SELECT 
        id, 
        masterMovieId, 
        title,
        ROW_NUMBER() OVER (PARTITION BY masterMovieId ORDER BY id) AS rn
    FROM 
        LetterboxdVImdb.dbo.MovieMaster
)
DELETE FROM CTE WHERE rn > 1;

USE LetterboxdVIMDb;
SELECT name 
FROM sys.key_constraints 
WHERE type = 'PK' AND OBJECT_NAME(parent_object_id) = 'MovieMaster';

ALTER TABLE MovieMaster DROP CONSTRAINT PK__MovieMas__3213E83FCD3C240F;

ALTER TABLE MovieMaster ALTER COLUMN masterMovieId NVARCHAR(255) NOT NULL;

ALTER TABLE MovieMaster ADD PRIMARY KEY (masterMovieId);

ALTER TABLE MovieMaster DROP COLUMN id;