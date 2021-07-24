all: build

warm:
  DOCKER_BUILDKIT=1 docker build .

build: warm
  docker run -v $PWD:/volume --rm $(DOCKER_BUILDKIT=1 docker build -q .) cargo build --release
