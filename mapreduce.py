from mrjob.job import MRJob
from mrjob.step import MRStep
import os

# define the path for benda.txt and mr_output.txt by joining the relative path and absolute path of this file in order to make sure this program can run in any computer
input_path = 'data\\benda.txt'
input_path = os.path.join(os.path.dirname(__file__), input_path)

output_path = 'data\mr_output.txt'
output_path = os.path.join(os.path.dirname(__file__), output_path)

class MapRed(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]

    def mapper(self, __, line):
        for word in line.split():
            yield(word, 1)

    def reducer(self, word, counts):
        yield(word, sum(counts))

if __name__ == '__main__':
    mr_job = MapRed(args=[input_path])

    # run the job and then convert the output into list and save the list into mr_output.txt
    with mr_job.make_runner() as runner:
        runner.run()

        # convert the output to list
        make_list = list(mr_job.parse_output(runner.cat_output()))
        
        # write every line into file by iterating key, value
        with open(output_path, "w") as f:
            for key, value in make_list:
                f.writelines(f"{key} {value}\n")
                
                print(key, value)
