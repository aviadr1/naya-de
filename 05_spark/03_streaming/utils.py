from pathlib import Path

folder = Path(__file__).parent.absolute()
output_folder = folder / 'output/'
output_folder.mkdir(exist_ok=True, parents=True)

sum_of_files_path = output_folder / 'sum_of_nums_files'
sum_of_files_path.mkdir(exist_ok=True, parents=True)

bible_path = output_folder / 'bible'
bible_path.mkdir(exist_ok=True, parents=True)