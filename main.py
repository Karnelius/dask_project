from distributed import Client
import re
import os
import time


def mapper(file):
    word_map = []
    for line in file:
        line = line.lower()
        words = [word for word in re.split("[^a-z']+", line) if word and word != "'"]
        for word in words:
            word_map.append(f'({word},1)')

    return word_map


def sorter(seq):
    seq.sort()
    return seq


def reducer(mapped_words):
    last_word = None
    word_count = 0
    counted_words = []

    for line in mapped_words:
        line = line[1:-1]
        word, count = line.split(',')
        count = int(count)

        if word == last_word:
            word_count += count
        else:
            if last_word:
                counted_words.append(f'{last_word} - {word_count}')

            word_count = count
            last_word = word

    counted_words.append(f'{last_word} - {word_count}')

    return counted_words


def presenter(data):
    data_dict = {value.split(' - ')[0]: int(value.split(' - ')[1]) for value in data}
    data_dict = {k: v for k, v in sorted(data_dict.items(), key=lambda item: item[1], reverse=True)}
    data_list = [f'{k} - {v}' for k, v in data_dict.items()]
    return data_list[:100]


def file_reader(path):
    files = [file for file in os.listdir(path)]
    content = []
    path = path if path.endswith('/') else path + '/'
    for file in files:
        for line in open(f'{path}{file}', encoding='ISO-8859-1'):
            content.append(line)

    return content, len(files)


def main():
    file_content, book_count = file_reader('./books/')

    start = time.time()
    client = Client('tcp://192.168.1.64:8786')

    dsk = {
        'content': file_content,
        'mapper': (mapper, 'content'),
        'sorter': (sorter, 'mapper'),
        'reducer': (reducer, 'sorter'),
        'presenter': (presenter, 'reducer')
    }

    result = client.get(dsk, 'presenter')
    end = time.time()
    for line in result:
        print(line)

    print(f'Counted the words in {book_count} book(s) in {round(end - start, 2)} second(s)')


if __name__ == '__main__':
    main()