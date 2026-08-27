import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TemplateCoverageTest {
    @Test
    fun everyTemplateHandlerIsLoadableAndNoRequirementIsDeclaredMissing() {
        templates().forEach { template ->
            val source = template.readText()
            assertFalse(source.contains("NotImplemented:"), "${template.name} declares an implementation gap")
            handlerRegex.findAll(source).forEach { match ->
                val handler = match.groupValues[1].substringBefore("::")
                val loaded = runCatching { Class.forName(handler, false, javaClass.classLoader) }
                assertTrue(
                    loaded.isSuccess,
                    "${template.name} references missing handler $handler: ${loaded.exceptionOrNull()}",
                )
            }
        }
    }

    @Test
    fun invokeTemplateGrantsChainedInvocationPermission() {
        val source = Path.of(projectDir(), "template_invoke.yaml").readText()
        assertTrue(
            source.contains("- lambda:InvokeFunction"),
            "template_invoke.yaml must allow its handlers to invoke target functions",
        )
    }

    private fun templates(): List<Path> =
        Files.list(Path.of(projectDir()))
            .use { paths ->
                paths.filter { it.fileName.toString().matches(Regex("template_.*\\.yaml")) }
                    .sorted()
                    .toList()
            }

    private fun projectDir(): String = requireNotNull(System.getProperty("conformance.projectDir"))

    private companion object {
        val handlerRegex: Regex = Regex("""(?m)^\s*Handler:\s*(\S+)\s*$""")
    }
}
