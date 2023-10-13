SELECT
    year,
    source,
    AvgRating,
    AvgRank,
    TotalWatched,
    CountInTop250
FROM (
    -- IMDb Data
    SELECT
        COALESCE(lbimdb.year, lblb.year) AS year,
        'IMDb' AS source,
        ROUND(AVG(COALESCE(imdbimdb.imdbRating, imdblb.imdbRating) / 2), 2) AS AvgRating, -- Halved IMDb Rating
        ROUND(SUM(COALESCE(imdbimdb.imdbRank, imdblb.imdbRank)) / NULLIF(COUNT(NULLIF(COALESCE(imdbimdb.imdbRank, imdblb.imdbRank), NULL)), 0), 2) AS AvgRank,
        SUM(COALESCE(imdbimdb.imdbWatched, imdblb.imdbWatched)) AS TotalWatched,
        SUM(CASE WHEN imdbimdb.masterMovieId IS NOT NULL THEN 1 ELSE 0 END) AS CountInTop250
    FROM 
        LetterboxdVImdb.dbo.MovieMaster mm
        LEFT JOIN LetterboxdVImdb.dbo.IMDbIMDbTop250 imdbimdb ON mm.masterMovieId = imdbimdb.masterMovieId
        LEFT JOIN LetterboxdVImdb.dbo.IMDbLetterboxdTop250 imdblb ON mm.masterMovieId = imdblb.masterMovieId
        LEFT JOIN LetterboxdVImdb.dbo.LBIMDbTop250 lbimdb ON mm.masterMovieId = lbimdb.masterMovieId
        LEFT JOIN LetterboxdVImdb.dbo.LBLetterboxdTop250 lblb ON mm.masterMovieId = lblb.masterMovieId
    GROUP BY
        COALESCE(lbimdb.year, lblb.year)

    UNION ALL

    -- Letterboxd Data
    SELECT
        COALESCE(lbimdb.year, lblb.year) AS year,
        'Letterboxd' AS source,
        ROUND(AVG(COALESCE(lbimdb.letterboxdRating, lblb.letterboxdRating)), 2) AS AvgRating,
        ROUND(SUM(COALESCE(lbimdb.letterboxdRank, lblb.letterboxdRank)) / NULLIF(COUNT(NULLIF(COALESCE(lbimdb.letterboxdRank, lblb.letterboxdRank), NULL)), 0), 2) AS AvgRank,
        SUM(COALESCE(lbimdb.letterboxdWatched, lblb.letterboxdWatched)) AS TotalWatched,
        SUM(CASE WHEN lblb.masterMovieId IS NOT NULL THEN 1 ELSE 0 END) AS CountInTop250
    FROM 
        LetterboxdVImdb.dbo.MovieMaster mm
        LEFT JOIN LetterboxdVImdb.dbo.LBIMDbTop250 lbimdb ON mm.masterMovieId = lbimdb.masterMovieId
        LEFT JOIN LetterboxdVImdb.dbo.LBLetterboxdTop250 lblb ON mm.masterMovieId = lblb.masterMovieId
    GROUP BY
        COALESCE(lbimdb.year, lblb.year)

) AS MainQuery