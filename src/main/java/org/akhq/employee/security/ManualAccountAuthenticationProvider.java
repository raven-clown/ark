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
import org.akhq.employee.service.ManualAccountService;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Map;
import java.util.Optional;

@Singleton
public class ManualAccountAuthenticationProvider<B> implements HttpRequestReactiveAuthenticationProvider<B> {

    @Inject
    private ManualAccountService manualAccountService;

    @Override
    public Publisher<AuthenticationResponse> authenticate(@Nullable HttpRequest<B> httpRequest,
                                                            AuthenticationRequest<String, String> authenticationRequest) {
        String employeeCode = String.valueOf(authenticationRequest.getIdentity());
        String rawPassword = String.valueOf(authenticationRequest.getSecret());

        return Mono.fromCallable(() -> manualAccountService.authenticate(employeeCode, rawPassword))
            .subscribeOn(Schedulers.boundedElastic())
            .map(this::toResponse)
            .onErrorResume(e -> Mono.just(new AuthenticationFailed("Manual account error: " + e.getMessage())));
    }

    private AuthenticationResponse toResponse(Optional<Employee> employee) {
        if (employee.isEmpty()) {
            return new AuthenticationFailed(AuthenticationFailureReason.CREDENTIALS_DO_NOT_MATCH);
        }
        Employee e = employee.get();
        return AuthenticationResponse.success(e.getEmployeeCode(), EmployeeAuthenticationProvider.rolesFor(e), Map.of(
            "auth_source", EmployeeAuthenticationProvider.AUTH_SOURCE,
            "employee_code", e.getEmployeeCode(),
            "full_name", e.getFullName(),
            "admin", e.isAdmin(),
            "super_admin", e.isSuperAdmin()
        ));
    }
}
