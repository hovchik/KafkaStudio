using KafkaStudio.Tests.Harness;
using KafkaStudio.Tests.Suites;

Console.WriteLine("KafkaStudio test suite (self-contained harness, no external test framework)");
Console.WriteLine("============================================================================");

var runner = new TestRunner();
LexerParserTests.Register(runner);
InterpreterTests.Register(runner);
JsonPathAndTemplateTests.Register(runner);
SchedulerTests.Register(runner);
RethrowEngineTests.Register(runner);
ViewModelTests.Register(runner);
SampleScriptsTests.Register(runner);

var exitCode = await runner.RunAllAsync();
return exitCode;
