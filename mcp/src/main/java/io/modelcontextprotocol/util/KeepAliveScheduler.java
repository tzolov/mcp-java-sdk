/**
 * Copyright 2025 - 2025 the original author or authors.
 */
package io.modelcontextprotocol.util;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class for scheduling regular keepAlive method calls using Project Reactor.
 * Provides both blocking and non-blocking keepAlive execution with configurable
 * intervals.
 *
 * @author Christian Tzolov
 */
public class KeepAliveScheduler {

	private static final Logger logger = LoggerFactory.getLogger(KeepAliveScheduler.class);

	/** The scheduler used for executing keepAlive calls */
	private final Scheduler scheduler;

	/** The current state of the scheduler */
	private final AtomicBoolean isRunning = new AtomicBoolean(false);

	/** The current subscription for the keepAlive calls */
	private Disposable currentSubscription;

	/**
	 * Creates a KeepAliveScheduler with a default single-threaded scheduler.
	 */
	public KeepAliveScheduler() {
		this(Schedulers.single());
	}

	/**
	 * Creates a KeepAliveScheduler with a custom scheduler.
	 * @param scheduler The scheduler to use for executing keepAlive calls
	 */
	public KeepAliveScheduler(Scheduler scheduler) {
		this.scheduler = scheduler;
	}

	/**
	 * Starts regular keepAlive calls with reactive keepAlive method.
	 * @param keepAliveMono A Mono representing the keepAlive operation
	 * @param initialDelay Initial delay before the first call
	 * @param interval The interval between calls
	 * @return Disposable to control the scheduled execution
	 */
	public Disposable start(Supplier<Mono<Void>> keepAliveMono, Duration initialDelay, Duration interval) {
		if (this.isRunning.compareAndSet(false, true)) {
			this.currentSubscription = Flux.interval(initialDelay, interval, scheduler)
				.flatMap(tick -> keepAliveMono.get().onErrorResume(error -> {
					logger.error("KeepAlive execution failed", error);
					return Mono.empty();
				}))
				.doOnCancel(() -> this.isRunning.set(false))
				.doOnComplete(() -> this.isRunning.set(false))
				.doOnError(error -> {
					logger.error("KeepAlive scheduler error", error);
					this.isRunning.set(false);
				})
				.subscribe();

			return this.currentSubscription;
		}
		else {
			throw new IllegalStateException("KeepAlive scheduler is already running. Stop it first.");
		}
	}

	/**
	 * Starts regular keepAlive calls with initial delay.
	 * @param keepAlive The keepAlive method to call
	 * @param initialDelay Initial delay before the first call
	 * @param interval The interval between subsequent calls
	 * @return Disposable to control the scheduled execution
	 */
	public Disposable start(Runnable keepAlive, Duration initialDelay, Duration interval) {
		if (this.isRunning.compareAndSet(false, true)) {
			this.currentSubscription = Flux.interval(initialDelay, interval, this.scheduler).doOnNext(tick -> {
				try {
					keepAlive.run();
				}
				catch (Exception e) {
					logger.error("KeepAlive execution failed", e);
				}
			})
				.doOnCancel(() -> this.isRunning.set(false))
				.doOnComplete(() -> this.isRunning.set(false))
				.doOnError(error -> {
					logger.error("KeepAlive scheduler error", error);
					this.isRunning.set(false);
				})
				.subscribe();

			return this.currentSubscription;
		}
		else {
			throw new IllegalStateException("KeepAlive scheduler is already running. Stop it first.");
		}
	}

	/**
	 * Stops the currently running keepAlive scheduler.
	 */
	public void stop() {
		if (this.currentSubscription != null && !this.currentSubscription.isDisposed()) {
			this.currentSubscription.dispose();
		}
		this.isRunning.set(false);
	}

	/**
	 * Checks if the scheduler is currently running.
	 * @return true if running, false otherwise
	 */
	public boolean isRunning() {
		return this.isRunning.get();
	}

	/**
	 * Shuts down the scheduler and releases resources.
	 */
	public void shutdown() {
		stop();
		if (this.scheduler instanceof Disposable) {
			((Disposable) this.scheduler).dispose();
		}
	}

	/**
	 * Example usage and demonstration of the KeepAliveScheduler.
	 */
	public static void main(String[] args) throws InterruptedException {
		// Example 1: Simple keepAlive with Runnable
		KeepAliveScheduler scheduler = new KeepAliveScheduler();

		Runnable keepAlive = () -> {
			System.out.println("KeepAlive called at: " + System.currentTimeMillis());
			// Simulate some work
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		};

		System.out.println("Starting keepAlive with 2-second interval...");
		Disposable subscription = scheduler.start(keepAlive, Duration.ofSeconds(1), Duration.ofSeconds(2));

		// Let it run for 10 seconds
		Thread.sleep(10000);

		System.out.println("Stopping keepAlive...");
		subscription.dispose();

		// Example 2: Reactive keepAlive with Mono
		System.out.println("\nStarting reactive keepAlive...");
		Supplier<Mono<Void>> reactiveKeepAlive = () -> Mono
			.fromRunnable(() -> System.out.println("Reactive KeepAlive: " + System.currentTimeMillis()))
			.then()
			.subscribeOn(Schedulers.boundedElastic());

		scheduler.start(reactiveKeepAlive, Duration.ofSeconds(1), Duration.ofSeconds(1));

		Thread.sleep(5000);

		scheduler.shutdown();
		System.out.println("Scheduler shut down.");
	}

}