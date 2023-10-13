SELECT
    country,
    ROUND(AVG(imdbRating), 2) AS AvgIMDbRating,
    ROUND(AVG(letterboxdRating), 2) AS AvgLetterboxdRating,
    ROUND(SUM(imdbRanked) / NULLIF(COUNT(NULLIF(imdbRanked, NULL)), 0), 2) AS AvgIMDbRank,  -- Weighted average
    ROUND(SUM(letterboxdRanked) / NULLIF(COUNT(NULLIF(letterboxdRanked, NULL)), 0), 2) AS AvgLetterboxdRank,  -- Weighted average
    SUM(imdbWatched) AS TotalIMDbWatched,
    SUM(letterboxdWatched) AS TotalLetterboxdWatched,
    SUM(CASE WHEN source IN ('LBIMDbTop250', 'Both') THEN 1 ELSE 0 END) AS CountInLBIMDbTop250,
    SUM(CASE WHEN source IN ('LBLetterboxdTop250', 'Both') THEN 1 ELSE 0 END) AS CountInLBLetterboxdTop250
FROM (
    SELECT
        COALESCE(lbimdb.country, lblb.country) AS country,
        COALESCE(imdbimdb.imdbRating, imdblb.imdbRating) / 2 AS imdbRating,
        COALESCE(lbimdb.letterboxdRating, lblb.letterboxdRating) AS letterboxdRating,
        COALESCE(imdbimdb.imdbRank, imdblb.imdbRank) AS imdbRanked,
        COALESCE(lbimdb.letterboxdRank, lblb.letterboxdRank) AS letterboxdRanked,
        COALESCE(imdbimdb.imdbWatched, imdblb.imdbWatched) AS imdbWatched,
        COALESCE(lbimdb.letterboxdWatched, lblb.letterboxdWatched) AS letterboxdWatched,
        CASE 
            WHEN lbimdb.masterMovieId IS NOT NULL AND lblb.masterMovieId IS NOT NULL THEN 'Both'
            WHEN lbimdb.masterMovieId IS NOT NULL THEN 'LBIMDbTop250' 
            WHEN lblb.masterMovieId IS NOT NULL THEN 'LBLetterboxdTop250' 
            ELSE NULL 
        END AS source
    FROM 
        LetterboxdVImdb.dbo.MovieMaster mm
    LEFT JOIN 
        LetterboxdVImdb.dbo.IMDbIMDbTop250 imdbimdb ON mm.masterMovieId = imdbimdb.masterMovieId
    LEFT JOIN 
        LetterboxdVImdb.dbo.IMDbLetterboxdTop250 imdblb ON mm.masterMovieId = imdblb.masterMovieId
    LEFT JOIN 
        LetterboxdVImdb.dbo.LBIMDbTop250 lbimdb ON mm.masterMovieId = lbimdb.masterMovieId
    LEFT JOIN 
        LetterboxdVImdb.dbo.LBLetterboxdTop250 lblb ON mm.masterMovieId = lblb.masterMovieId
) AS SubQuery
GROUP BY
    country