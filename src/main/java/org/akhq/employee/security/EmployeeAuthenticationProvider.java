package org.akhq.employee.security;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.AuthenticationFailed;
import io.micronaut.security.authentication.AuthenticationFailureReason;
import io.micronaut.security.authentication.AuthenticationRequest;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.provider.HttpRequestReactiveAuthenticationProvider;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.akhq.employee.domain.Employee;
import org.akhq.employee.service.EmployeeAuthenticationService;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Singleton
public class EmployeeAuthenticationProvider<B> implements HttpRequestReactiveAuthenticationProvider<B> {
    public static final String AUTH_SOURCE = "EMPLOYEE_DIRECTORY";

    @Inject
    private EmployeeAuthenticationService authenticationService;

    @Override
    public Publisher<AuthenticationResponse> authenticate(@Nullable HttpRequest<B> httpRequest,
                                                            AuthenticationRequest<String, String> authenticationRequest) {
        String employeeCode = String.valueOf(authenticationRequest.getIdentity());

        return Mono.fromCallable(() -> authenticationService.authenticate(employeeCode))
            .subscribeOn(Schedulers.boundedElastic())
            .map(this::toResponse)
            .onErrorResume(e -> Mono.just(new AuthenticationFailed("Employee directory error: " + e.getMessage())));
    }

    private AuthenticationResponse toResponse(Optional<Employee> employee) {
        if (employee.isEmpty()) {
            return new AuthenticationFailed(AuthenticationFailureReason.USER_NOT_FOUND);
        }
        Employee e = employee.get();
        return AuthenticationResponse.success(e.getEmployeeCode(), rolesFor(e), Map.of(
            "auth_source", AUTH_SOURCE,
            "employee_code", e.getEmployeeCode(),
            "full_name", e.getFullName(),
            "admin", e.isAdmin(),
            "super_admin", e.isSuperAdmin()
        ));
    }

    public static List<String> rolesFor(Employee e) {
        if (e.isSuperAdmin()) {
            return List.of("ROLE_EMPLOYEE", "ROLE_ADMIN", "ROLE_SUPER_ADMIN");
        }
        if (e.isAdmin()) {
            return List.of("ROLE_EMPLOYEE", "ROLE_ADMIN");
        }
        return List.of("ROLE_EMPLOYEE");
    }
}
