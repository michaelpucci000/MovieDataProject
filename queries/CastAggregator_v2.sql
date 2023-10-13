SELECT 
    castMember,
    source,
    AvgRating,
    AvgRank,
    TotalWatched,
    CountInTop250
FROM (
    -- IMDb Data
    SELECT
        value AS castMember,
        'IMDb' AS source,
        ROUND(AVG(imdbRating), 2) AS AvgRating,
        ROUND(SUM(imdbRanked) / NULLIF(COUNT(NULLIF(imdbRanked, NULL)), 0), 2) AS AvgRank,
        SUM(imdbWatched) AS TotalWatched,
        SUM(CASE WHEN source IN ('LBIMDbTop250', 'Both') THEN 1 ELSE 0 END) AS CountInTop250
    FROM (
        SELECT
            COALESCE(lbimdb.cast, lblb.cast) AS combinedCast,
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
            LEFT JOIN LetterboxdVImdb.dbo.IMDbIMDbTop250 imdbimdb ON mm.masterMovieId = imdbimdb.masterMovieId
            LEFT JOIN LetterboxdVImdb.dbo.IMDbLetterboxdTop250 imdblb ON mm.masterMovieId = imdblb.masterMovieId
            LEFT JOIN LetterboxdVImdb.dbo.LBIMDbTop250 lbimdb ON mm.masterMovieId = lbimdb.masterMovieId
            LEFT JOIN LetterboxdVImdb.dbo.LBLetterboxdTop250 lblb ON mm.masterMovieId = lblb.masterMovieId
    ) AS IMDbSubquery
    CROSS APPLY STRING_SPLIT(combinedCast, ',')
    GROUP BY
        value

    UNION ALL

    -- Letterboxd Data
    SELECT
        value AS castMember,
        'Letterboxd' AS source,
        ROUND(AVG(letterboxdRating), 2) AS AvgRating,
        ROUND(SUM(letterboxdRanked) / NULLIF(COUNT(NULLIF(letterboxdRanked, NULL)), 0), 2) AS AvgRank,
        SUM(letterboxdWatched) AS TotalWatched,
        SUM(CASE WHEN source IN ('LBLetterboxdTop250', 'Both') THEN 1 ELSE 0 END) AS CountInTop250
    FROM (
        SELECT
            COALESCE(lbimdb.cast, lblb.cast) AS combinedCast,
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
            LEFT JOIN LetterboxdVImdb.dbo.IMDbIMDbTop250 imdbimdb ON mm.masterMovieId = imdbimdb.masterMovieId
            LEFT JOIN LetterboxdVImdb.dbo.IMDbLetterboxdTop250 imdblb ON mm.masterMovieId = imdblb.masterMovieId
            LEFT JOIN LetterboxdVImdb.dbo.LBIMDbTop250 lbimdb ON mm.masterMovieId = lbimdb.masterMovieId
            LEFT JOIN LetterboxdVImdb.dbo.LBLetterboxdTop250 lblb ON mm.masterMovieId = lblb.masterMovieId
    ) AS LetterboxdSubquery
    CROSS APPLY STRING_SPLIT(combinedCast, ',')
    GROUP BY
        value

) AS MainQuery