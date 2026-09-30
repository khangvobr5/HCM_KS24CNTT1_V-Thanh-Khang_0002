package org.example.bookingservice.models.services.impl;

import lombok.RequiredArgsConstructor;
import org.example.bookingservice.models.constants.BookingStatus;
import org.example.bookingservice.models.dto.requests.CreateBookingDetailRequest;
import org.example.bookingservice.models.dto.requests.CreateBookingRequest;
import org.example.bookingservice.models.dto.responses.BookingDetailResponse;
import org.example.bookingservice.models.dto.responses.BookingResponse;
import org.example.bookingservice.models.dto.responses.MovieResponse;
import org.example.bookingservice.models.entities.Booking;
import org.example.bookingservice.models.entities.BookingDetail;
import org.example.bookingservice.models.repositories.BookingDetailRepository;
import org.example.bookingservice.models.repositories.BookingRepository;
import org.example.bookingservice.models.services.BookingService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class BookingServiceImpl implements BookingService {

        private final BookingRepository bookingRepository;
        private final BookingDetailRepository bookingDetailRepository;
        private final MovieGatewayService movieGatewayService;
        private final KafkaTemplate<String, String> kafkaTemplate;

        @Override
        @Transactional
        public BookingResponse createBooking(CreateBookingRequest request) {
                Booking booking = Booking.builder()
                        .customerName(request.customerName())
                        .customerEmail(request.customerEmail())
                        .status(BookingStatus.PENDING)
                        .total(0.0)
                        .build();

                booking = bookingRepository.save(booking);

                double total = 0.0;
                List<BookingDetailResponse> itemResponses = new ArrayList<>();

                for (CreateBookingDetailRequest itemReq : request.items()) {
                        MovieResponse movie = movieGatewayService.getMovieById(itemReq.movieId());

                        double subtotal = movie.ticketPrice() * itemReq.quantity();
                        total += subtotal;

                        BookingDetail detail = BookingDetail.builder()
                                .booking(booking)
                                .movieId(movie.id())
                                .quantity(itemReq.quantity())
                                .unitPrice(movie.ticketPrice())
                                .build();
                        
                        detail = bookingDetailRepository.save(detail);

                        itemResponses.add(new BookingDetailResponse(
                                detail.getId(),
                                movie.id(),
                                movie.title(),
                                itemReq.quantity(),
                                movie.ticketPrice(),
                                subtotal
                        ));
                }

                booking.setTotal(total);
                bookingRepository.save(booking);

                kafkaTemplate.send("booking-created", booking.getCustomerEmail());

                return new BookingResponse(
                        booking.getId(),
                        booking.getCustomerName(),
                        booking.getCustomerEmail(),
                        booking.getTotal(),
                        booking.getStatus(),
                        itemResponses
                );
        }
}
