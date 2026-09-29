using KafkaStudio.Tests.Harness;
using KafkaStudio.Tests.Suites;

Console.WriteLine("KafkaStudio test suite (self-contained harness, no external test framework)");
Console.WriteLine("============================================================================");

// Never touch the real %APPDATA%/KafkaStudio settings from tests (connections, tasks, rules...).
var dataDir = Path.Combine(Path.GetTempPath(), $"kafkastudio-tests-{Guid.NewGuid():N}");
KafkaStudio.Core.Persistence.JsonFileStore.DataDirectory = dataDir;

var runner = new TestRunner();
LexerParserTests.Register(runner);
InterpreterTests.Register(runner);
JsonPathAndTemplateTests.Register(runner);
SchedulerTests.Register(runner);
RethrowEngineTests.Register(runner);
ViewModelTests.Register(runner);
SampleScriptsTests.Register(runner);
RegressionTests.Register(runner);
DataSearchTests.Register(runner);
DataSearchViewModelTests.Register(runner);
QaLanguageTests.Register(runner);
QaEngineTests.Register(runner);
QaLabViewModelTests.Register(runner);

var exitCode = await runner.RunAllAsync();
try { Directory.Delete(dataDir, recursive: true); } catch { /* best effort */ }
return exitCode;
