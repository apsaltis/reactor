package reactor.graph

import reactor.core.Environment
import reactor.function.Predicate
import spock.lang.Specification

/**
 * @author Jon Brisbin
 */
class GraphsSpec extends Specification {

	Environment env

	def setup() {
		env = new Environment()
	}

	def "Graphs provide wiring of actions"() {

		given: "a Graph"
			int length = 0, otherwiseLength = 0
			Graph<String> graph = Graph.create(env, "sync")

		when: "a Node and Route is defined and data is accepted"
			graph.node().
					when({ String s -> s.startsWith("Hello") } as Predicate<String>).
					end({ s -> length = s.size() }).
					otherwise().
					end({ s -> otherwiseLength = s.size() })
			graph.accept("Hello World!")
			graph.accept("Goodbye World!")

		then: "the Consumer was invoked"
			length == 12
			otherwiseLength == 14

	}

	def "Graphs provide routing based on Predicates"() {

		given: "a Graph"
			int helloLength = 0, goodbyeLength = 0
			Graph<String> graph = Graph.create(env, "sync")

		when: "Routes are defined that transfer control from one to another"
			graph.node("count.hello").then({ s -> helloLength = s.size() })
			graph.node("count.goodbye").then({ s -> goodbyeLength = s.size() })
			graph.node("start").
					when({ String s -> s.startsWith("Hello") } as Predicate<String>).
					routeTo("count.hello").
					otherwise().
					routeTo("count.goodbye")
			graph.startIn("start")
			graph.accept("Hello World!")
			graph.accept("Goodbye World!")

		then: "counts should be correct"
			helloLength == 12
			goodbyeLength == 14

	}

}
