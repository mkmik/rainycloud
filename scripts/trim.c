#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <fenv.h>
#include <math.h>

#define LINE_LENGTH 1024


char line[LINE_LENGTH];
char num_buf[LINE_LENGTH];

inline void truncate(char* l) {
  int commas = 0;
  int skip = 500;

  while(*l) {
    if(*l == ',')
      commas++;

    if(commas == 2) {
      l++;

      // skip precision numer
      l--;
      *l = 0;

      char* precision_s = l+1;
      while(*l != ',')
        l++;
      *l = 0;

      double precision_d;
      sscanf(precision_s, "%lf", &precision_d);
      //double r = nearbyint(precision_d*1000000) / 1000000.0;
      double r = round(precision_d*1000000) / 1000000.0;

      sprintf(num_buf,"%lf", r);
      //sprintf(num_buf,"%0.6lf", precision_d);

      char *num_buf_end = num_buf + strlen(num_buf) - 1;
      while(*num_buf_end == '0')
        *num_buf_end-- = 0;
      if(*num_buf_end == '.')
        *num_buf_end-- = 0;

      //if(strlen(precision_s) != 8)

      strcpy(precision_s, num_buf);
      *(precision_s-1) = ',';

      break;

    }

    l++;
  }
}

void process_lines(char* file, size_t size) {
  //fesetround (FE_TONEAREST);
  fesetround (FE_UPWARD);
  while(1) {
    const char* begin = file;

    while(size-- && *file++)
      if(*file == '\n')
        break;

    // last empty line
    if(begin == file)
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
