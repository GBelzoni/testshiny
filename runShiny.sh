#!/bin/bash

docker run --rm -p 80:3838 \
-v /home/ec2-user/testshiny/testShiny/:/srv/shiny-server/ \
-v /home/ec2-user/shinylog/:/var/log/shiny-server/ \
rocker/shiny
}
