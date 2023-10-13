import gzip
import shutil

def unzip_gz_file(input_file, output_file):
    try:
        with gzip.open(input_file, 'rb') as f_in:
            with open(output_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    except:
        print('Error during unzipping file')

#Specify the input and output file names
input_file = input('Enter file name:')

start_index = input_file.find('MovieProject') + len('MovieProject') + 1
end_index = input_file.find('.tsv')

# Extract the characters between 'Movie Project' and '.tsv'
extracted_characters = input_file[start_index:end_index]

# Append ".csv" to the extracted characters
output_file = extracted_characters + ".csv"

#Call the function to unzip the file
unzip_gz_file(input_file, output_file)
