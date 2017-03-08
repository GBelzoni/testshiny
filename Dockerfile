FROM rocker/shiny

RUN Rscript -e "install.packages('ggplot2')"
RUN Rscript -e "install.packages('dplyr')"
RUN Rscript -e "install.packages('tidyr')"
RUN Rscript -e "install.packages('ggplot2')"
RUN Rscript -e "install.packages('RMySQL')"
RUN Rscript -e "install.packages('zoo')"
RUN Rscript -e "install.packages('stringr')"
RUN Rscript -e "install.packages('lubridate')"













