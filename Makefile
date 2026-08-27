.PHONY: build-%

build-%:
	@test -n "$(ARTIFACTS_DIR)" || (echo "ARTIFACTS_DIR is required" >&2; exit 2)
	./gradlew --no-daemon :conformance-tests:shadowJar
	install -d "$(ARTIFACTS_DIR)/lib"
	install -m 0644 \
		conformance-tests/build/libs/conformance-tests-0.1.0-SNAPSHOT-all.jar \
		"$(ARTIFACTS_DIR)/lib/conformance-tests.jar"
