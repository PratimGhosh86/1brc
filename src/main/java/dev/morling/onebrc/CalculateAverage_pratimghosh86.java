package dev.morling.onebrc;

import static java.util.stream.Collectors.groupingBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;

public class CalculateAverage_pratimghosh86 {

	public static void main(String... args) throws IOException {
		final var start = System.nanoTime();
		try (final var reader = Files
				.newBufferedReader(Path.of(new File("src/../measurements.txt").getAbsolutePath()))) {
			reader.lines().parallel().map(l -> new Temperature(l.split(";")))
					.collect(groupingBy(m -> m.station(), COLLECTOR)).entrySet().stream().forEach(System.out::println);
		}
		final var stop = System.nanoTime();
		System.out.println(
				String.format("Total time: %ss", TimeUnit.SECONDS.convert(stop - start, TimeUnit.NANOSECONDS)));
	}

	private static final Collector<Temperature, TemperatureAggregator, TemperatureResult> COLLECTOR = Collector
			.of(TemperatureAggregator::new, accumulate(), combine(), finisher());

	private static Function<TemperatureAggregator, TemperatureResult> finisher() {
		return agg -> new TemperatureResult(agg.min, (Math.round(agg.sum * 10.0) / 10.0f) / agg.count, agg.max);
	}

	private static BinaryOperator<TemperatureAggregator> combine() {
		return (agg1, agg2) -> {
			var res = new TemperatureAggregator();
			res.min = Math.min(agg1.min, agg2.min);
			res.max = Math.max(agg1.max, agg2.max);
			res.sum = agg1.sum + agg2.sum;
			res.count = agg1.count + agg2.count;
			return res;
		};
	}

	private static BiConsumer<TemperatureAggregator, Temperature> accumulate() {
		return (a, m) -> {
			a.min = Math.min(a.min, m.value);
			a.max = Math.max(a.max, m.value);
			a.sum += m.value;
			a.count++;
		};
	}

	private static class TemperatureAggregator {
		private float min = Float.POSITIVE_INFINITY;
		private float max = Float.NEGATIVE_INFINITY;
		private float sum;
		private long count;
	}

	private static record Temperature(String station, float value) {
		public Temperature(String... parts) {
			this(parts[0], Float.parseFloat(parts[1]));
		}
	}

	private static record TemperatureResult(float min, float mean, float max) {

		public String toString() {
			return String.format("%s/%s/%s", round(min), round(mean), round(max));
		}

		private float round(float value) {
			return Math.round(value * 10.0) / 10.0f;
		}
	}

}
