#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <string.h>
#include <stdio.h>
#include <stdlib.h>


#define LINE_LENGTH 1024


char line[LINE_LENGTH];
char num_buf[LINE_LENGTH];

inline void truncate(char* l) {
  int commas = 0;
  int skip = 500;

  while(*l) {
    if(*l == ',')
      commas++;

    if(commas == 3) {
      *l = 0;
      break;
    }

    l++;
  }
}

void process_lines(char* file, size_t size) {
  while(1) {
    const char* begin = file;

    while(size-- && *file++)
      if(*file == '\n')
        break;

    strncpy(line, begin, file-begin);
    line[file-begin-1] = 0;

    truncate(line);
    puts(line);

    size--;
    if(!*file++)
      break;
  }
}

int main(int argc, char **argv) {
  if(argc < 2) {
    printf("Missing input file name\n");
    exit(1);
  }

  return mmap_main(argc, argv);
}

int mmap_main(int argc, char **argv) {
  int fd = open(argv[1], 0);
  if(fd == 0) {
    printf("cannot open file %s\n", argv[1]);
    exit(1);
  }

  struct stat stat;
  fstat(fd, &stat);

  size_t size = stat.st_size;

  char* file = mmap(NULL, size, PROT_READ, MAP_SHARED|MAP_NORESERVE|MAP_POPULATE, fd, 0);

  if(file == MAP_FAILED) {
    perror("cannot mmap file");
    exit(1);
  }

  process_lines(file, size);

  munmap(file, size);
  close(fd);
  return 0;
}


int read_main(int argc, char **argv) {
  FILE *inFilePtr = fopen(argv[1], "r+");
  if(inFilePtr == NULL) {
    printf("cannot open file %s\n", argv[1]);
    exit(1);
  }

  while (!feof(inFilePtr)){
    fscanf(inFilePtr, "%s", line);
    truncate(line);
    puts(line);
  }
  return 0;
}
