suppressPackageStartupMessages(library(data.table))
setDTthreads(percent = 100)
#suppressPackageStartupMessages(suppressWarnings(library(kableExtra)))
suppressPackageStartupMessages(suppressWarnings(library(dplyr, warn.conflicts = FALSE)))
options(dplyr.summarise.inform = FALSE)
library(tidyr)

library("odbc")
library("DBI")


title.basics <- as.data.frame(
  fread(
    "/r-code/imdb/title.basics.tsv",
    quote = "",
    na.strings = "\\N",
    showProgress = FALSE
  )
)

title.ratings <-
  as.data.frame(
    fread(
      "/r-code/imdb/title.ratings.tsv",
      quote = "",
      na.strings = "\\N",
      showProgress = FALSE
    )
  )

imdb_movies <- as.data.frame(read.csv("/r-code/imdb/movies.csv",
                                      sep = ',',
                                      header = TRUE))

names(imdb_movies)[1] <- "tconst"

imdb_movies$country <- gsub("  ", " ", imdb_movies$country)
imdb_movies$country <- gsub(", ", ",", imdb_movies$country)

imdb_movies$language <- gsub("  ", " ", imdb_movies$language)
imdb_movies$language <- gsub(", ", ",", imdb_movies$language)

imdb_movies$director <- gsub("  ", " ", imdb_movies$director)
imdb_movies$director <- gsub(", ", ",", imdb_movies$director)

imdb_movies$actors <- gsub("  ", " ", imdb_movies$actors)
imdb_movies$actors <- gsub(", ", ",", imdb_movies$actors)

mubi_movie_data <-
  as.data.frame(read.csv(
    "/r-code/mubi/mubi_movie_data.csv",
    sep = ',',
    header = TRUE
  ))


mubi_ratings_data <-
  as.data.frame(read.csv(
    "/r-code/mubi/mubi_ratings_data.csv",
    sep = ',',
    header = TRUE
  ))

mubi <-
  merge(mubi_movie_data[, c("movie_id", "movie_title", "movie_release_year")],
        aggregate(. ~ movie_id, data = mubi_ratings_data[, c("movie_id", "rating_score")], mean),
        by = "movie_id")
mubi <-
  merge(mubi,
        aggregate(. ~ movie_id, data = mubi_ratings_data[, c("movie_id", "rating_score")], length),
        by = "movie_id")

names(mubi)[2] <- "originalTitle"
names(mubi)[3] <- "year"
names(mubi)[4] <- "mubi_score"
names(mubi)[5] <- "mubi_votes"


normalized_score <- mubi$mubi_score * 2
vote_adjusted_score <-
  normalized_score + (log(mubi$mubi_votes, base = 10) / 10)
mubi$mubi_score <- round(vote_adjusted_score, digits = 2)


imdb <- merge(title.basics, title.ratings, by = "tconst")


names(imdb)[6] <- "year"
imdb$year <- as.integer(imdb$year)


imdb$runtimeMinutes <- as.integer(imdb$runtimeMinutes)

names(imdb)[10] <- "imdb_score"
imdb$imdb_score <- as.numeric(imdb$imdb_score)

names(imdb)[11] <- "imdb_votes"
imdb$imdb_votes <- as.integer(imdb$imdb_votes)

vote_adjusted_score <-
  imdb$imdb_score + (log(imdb$imdb_votes, base = 10) / 10)
imdb$imdb_score <- round(vote_adjusted_score, digits = 2)


imdb <- subset(imdb, select = -c(isAdult, endYear))

imdb <-
  merge(imdb, imdb_movies[, c("tconst", "country", "language", "director", "actors")], by = "tconst")


names(mubi)[2] <- "lowerOriginalTitle"
mubi$lowerOriginalTitle <- tolower(mubi$lowerOriginalTitle)

imdb$lowerOriginalTitle <- tolower(imdb$originalTitle)
imdb$lowerPrimaryTitle <- tolower(imdb$primaryTitle)

tmp1 <- imdb %>% right_join(mubi, by = c("lowerOriginalTitle")) %>%
  filter(abs(year.x - year.y) <= 1)


names(mubi)[2] <- "lowerPrimaryTitle"
tmp2 <- imdb %>% right_join(mubi, by = c("lowerPrimaryTitle")) %>%
  filter(abs(year.x - year.y) <= 1)

imdbmubi <- unique(rbind(tmp1, tmp2))



imdbmubi <-
  subset(imdbmubi,
         select = -c(movie_id, lowerOriginalTitle, lowerPrimaryTitle))
names(imdbmubi)[5] <- "year"

imdbmubi <- subset(imdbmubi, select = -c(year.y))

imdbmubi_clean <- imdbmubi %>%
  drop_na(primaryTitle) %>%
  drop_na(imdb_score) %>%
  drop_na(imdb_votes) %>%
  drop_na(mubi_score) %>%
  drop_na(mubi_votes) %>%
  drop_na(runtimeMinutes)


imdbmubi_clean$total <-
  round((imdbmubi_clean$mubi_score + imdbmubi_clean$imdb_score) / 2,
        digits = 2)




con <- dbConnect(odbc::odbc(), "MoviesDSN")


if (dbExistsTable(con, "all_movie_data")) {
  dbRemoveTable(con, "all_movie_data")
}

dbWriteTable(con,
             name = "all_movie_data",
             value = imdbmubi_clean,
             row.names = FALSE)
dbDisconnect(conn = con)
