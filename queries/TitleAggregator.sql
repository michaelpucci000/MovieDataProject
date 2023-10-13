SELECT
    mm.masterMovieId,
    mm.title,
    COALESCE(imdbimdb.imdbRating, imdblb.imdbRating) / 2 AS imdbRating, -- Divided by 2 to scale out of 5
    COALESCE(lbimdb.letterboxdRating, lblb.letterboxdRating) AS letterboxdRating,
    COALESCE(imdbimdb.imdbRank, imdblb.imdbRank) AS imdbRanked,
    COALESCE(lbimdb.letterboxdRank, lblb.letterboxdRank) AS letterboxdRanked,
    COALESCE(imdbimdb.imdbWatched, imdblb.imdbWatched) AS imdbWatched,
    COALESCE(lbimdb.letterboxdWatched, lblb.letterboxdWatched) AS letterboxdWatched
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