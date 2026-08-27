.PHONY: build-%

build-%:
	./gradlew :conformance-tests:shadowJar
	mkdir -p "$(ARTIFACTS_DIR)/lib"
	cp conformance-tests/build/libs/conformance-tests-0.1.0-SNAPSHOT-all.jar "$(ARTIFACTS_DIR)/lib/"
