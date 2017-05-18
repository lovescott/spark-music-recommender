# Alternating Least Squares Music Recommender


## Table of Contents

- [Installation](#installation)
  - [Support](#support)
  - [Contributing](#contributing)

## Installation
  This ALS example makes use of the AudioScrobbler dataset that can be downloaded [here](http://www-etud.iro.umontreal.ca/~bergstrj/audioscrobbler_data.html)

  The spark application is setup to run local and assumes that the needed files are in a directory name 'data' in the root of the project.

  Clone and build with SBT:

  ```sh
  git clone https://github.com/lovescott/spark-recommender.git
  cd spark-recommender/
  sbt package
  spark-submit --class MusicRecommender --master local target/scala-2.11/spark-recommender_2.11-1.0.jar
  ```


## Support

  Please [open an issue](https://github.com/lovescott/spark-recommender/issues/new) for support.

## Contributing

  Please contribute using [Github Flow](https://guides.github.com/introduction/flow/). Create a branch, add commits, and [open a pull request](https://github.com/lovescott/spark-recommender/compare).
