package org.example.bookingservice.models.services.impl;

import feign.FeignException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import org.example.bookingservice.clients.MovieClient;
import org.example.bookingservice.exceptions.MovieNotFoundException;
import org.example.bookingservice.exceptions.MovieServiceException;
import org.example.bookingservice.models.dto.responses.MovieResponse;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class MovieGatewayService {

    private final MovieClient movieClient;

    @CircuitBreaker(name = "movieService", fallbackMethod = "getMovieByIdFallback")
    public MovieResponse getMovieById(Long movieId) {
        return movieClient.getMovieById(movieId);
    }

    public MovieResponse getMovieByIdFallback(Long movieId, Throwable throwable) {
        if (throwable instanceof FeignException && ((FeignException) throwable).status() == 404) {
            throw new MovieNotFoundException(movieId);
        }
        throw new MovieServiceException("Movie Service is unavailable");
    }
}
