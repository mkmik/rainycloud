#include <stdio.h>
#include <stdlib.h>

#define LINE_LENGTH 1024

char line[LINE_LENGTH];

inline void truncate(char* l) {
  int commas = 0;
  int skip = 5;
   
  while(*l) {
    if(*l == ',')
      commas++;

    if(commas == 2) {
      l++;
      /*
        if(*l == '1') {
        l++;
        *l = 0;
        return;
      }
      */
      while(skip-- && *l != ',')
        l++;
      *l = 0;
      l--;
      while(*l == '0')
        *l-- = 0;
      if(*l == '.')
        *l = 0;
    }
    
    l++;
  }
}

int main(int argc, char **argv) {
  if(argc < 1) {
    printf("Missing input file name\n");
    exit(1);
  }

  FILE *inFilePtr = fopen(argv[1], "r+");
  while (!feof(inFilePtr)){
    fscanf(inFilePtr, "%s", line);
    //    truncate(line);
    //printf("%s\n", line);
    //puts(line);
  }
  return 0;
}
