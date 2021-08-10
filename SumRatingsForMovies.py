from mrjob.job import MRJob
from mrjob.step import MRStep


class SumRatingsForMovies(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_movies, combiner=self.combine_movie_rating_counts,
                reducer=self.reducer_sum_rating_counts
            ),
            MRStep(
                reducer=self.reducer_sort_counts
            )
        ]
    """test"""
    def mapper_get_movies(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')

        yield movieID, 1

    def combine_movie_rating_counts(self, key, values):
        yield key, sum(values)

    def reducer_sum_rating_counts(self, key, values):
        yield None, (sum(values), key)

    def reducer_sort_counts(self, _, values):
        for count, key in sorted(values, reverse=True):
            yield int(count), key


if __name__ == '__main__':
    SumRatingsForMovies.run()
