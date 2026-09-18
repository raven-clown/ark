package org.akhq.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.util.StringUtils;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.authentication.AuthorizationException;
import io.micronaut.security.utils.SecurityService;
import jakarta.inject.Inject;
import org.akhq.configs.security.Group;
import org.akhq.configs.security.SecurityProperties;
import org.akhq.employee.repository.EmployeeRepository;
import org.akhq.employee.security.EmployeeAuthenticationProvider;
import org.akhq.employee.service.EffectiveGrant;
import org.akhq.employee.service.PermissionResolutionService;
import org.akhq.models.security.ClaimProvider;
import org.akhq.security.annotation.AKHQSecured;
import org.akhq.security.rule.AKHQSecurityRule;

import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

abstract public class AbstractController {

    private static final StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    protected SecurityProperties securityProperties;

    @Inject
    private ClaimProvider claimProvider;

    @Inject
    private EmployeeRepository employeeRepository;

    @Inject
    private PermissionResolutionService permissionResolutionService;

    @Value("${micronaut.server.context-path:}")
    protected String basePath;

    protected String getBasePath() {
        return basePath.replaceAll("/$", "");
    }

    protected URI uri(String path) throws URISyntaxException {
        return new URI((this.basePath != null ? this.basePath : "") + path);
    }

    protected List<Group> getUserGroups() {
        // Authentication disabled, no groups to return
        if (!applicationContext.containsBean(SecurityService.class)) {
            return List.of();
        }

        Optional<Authentication> authentication = applicationContext.getBean(SecurityService.class).getAuthentication();

        var groups = new ArrayList<Group>();

        // Add the default group if there is one
        if (securityProperties.getGroups().get(securityProperties.getDefaultGroup()) != null) {
            groups.addAll(securityProperties.getGroups().get(securityProperties.getDefaultGroup()));
        }

        // Add user groups. Employees are grouped separately below since their permissions come
        // from the RBAC database rather than the akhq.security.groups/roles YAML, and their JWT
        // carries no "groups" claim for AKHQSecurityRule.unrollGroups to parse.
        authentication.ifPresent(value -> {
            if (isEmployeeAuthentication(value)) {
                groups.addAll(getEmployeeEffectiveGrants(value).stream()
                    .map(this::toSyntheticGroup)
                    .toList());
            } else {
                groups.addAll(
                    AKHQSecurityRule.unrollGroups(value, claimProvider).values().stream()
                        .flatMap(Collection::stream)
                        .map(gb -> new ObjectMapper().convertValue(gb, Group.class))
                        .toList());
            }
        });

        return groups;
    }

    /**
     * Only safe to use where the caller reads Group.clusters directly (e.g. cluster listing) -
     * the role name carries no meaning in akhq.security.roles, so anything resolving permissions
     * through securityProperties.getRoles().get(group.getRole()) must branch on
     * isEmployeeAuthentication() instead of consuming this.
     */
    private Group toSyntheticGroup(EffectiveGrant grant) {
        Group group = new Group();
        group.setRole("employee-grant:" + grant.resource() + ":" + grant.action());
        group.setClusters(List.of(grant.clusterPattern()));
        group.setPatterns(List.of(grant.topicPattern()));
        return group;
    }

    protected boolean isEmployeeAuthentication(Authentication authentication) {
        return authentication != null
            && EmployeeAuthenticationProvider.AUTH_SOURCE.equals(authentication.getAttributes().get("auth_source"));
    }

    protected List<EffectiveGrant> getEmployeeEffectiveGrants(Authentication authentication) {
        String employeeCode = String.valueOf(authentication.getAttributes().get("employee_code"));
        return employeeRepository.findByEmployeeCode(employeeCode)
            .map(employee -> permissionResolutionService.resolveEffectiveGrants(employee.getId()))
            .orElse(List.of());
    }

    /**
     * Build a list of regex based on the user's groups patterns attribute and the current cluster
     *
     * @param cluster
     * @return
     */
    protected List<String> buildUserBasedResourceFilters(String cluster) {
        // Authentication disabled, we allow everything
        if (!applicationContext.containsBean(SecurityService.class))
            return List.of();

        AKHQSecured annotation;
        try {
            annotation = getCallingAKHQSecuredAnnotation();
        } catch (NoSuchMethodException e) {
            return List.of();
        }

        Optional<Authentication> authentication = applicationContext.getBean(SecurityService.class).getAuthentication();
        if (authentication.isPresent() && isEmployeeAuthentication(authentication.get())) {
            return getEmployeeEffectiveGrants(authentication.get()).stream()
                .filter(grant -> grant.resource() == annotation.resource() && grant.action() == annotation.action())
                .filter(grant -> Pattern.matches(grant.clusterPattern(), cluster))
                .map(EffectiveGrant::topicPattern)
                .distinct()
                .collect(Collectors.toList());
        }

        return getUserGroups().stream()
            // Keep only group matching the cluster
            .filter(group -> group.getClusters()
                .stream()
                .anyMatch(c -> Pattern.matches(c, cluster)))
            // Iterate over all the roles of the user remaining groups to extract the restriction attribute for the
            // given cluster and resource
            .map(gb -> securityProperties.getRoles().get(gb.getRole())
                .stream()
                // Find roles with a resource and action matching the calling method AKHQSecured annotation
                .filter(role -> role.getResources().contains(annotation.resource())
                    && role.getActions().contains(annotation.action()))
                // Keep only the restriction attribute containing the patterns
                .map(role -> gb.getPatterns())
                .collect(Collectors.toList()))
            .flatMap(Collection::stream)
            .flatMap(Collection::stream)
            .distinct()
            .collect(Collectors.toList());
    }

    private AKHQSecured getCallingAKHQSecuredAnnotation() throws NoSuchMethodException {
        StackWalker.StackFrame sf = walker.walk(frames ->
            frames.filter(frame -> frame.getDeclaringClass().equals(getClass()))
                .findFirst()
                .orElseThrow());

        Method method = sf.getDeclaringClass().getDeclaredMethod(sf.getMethodName(), sf.getMethodType().parameterArray());
        AKHQSecured annotation;

        // Take the method annotation is present
        if (method.isAnnotationPresent(AKHQSecured.class)) {
            annotation = method.getAnnotation(AKHQSecured.class);
        } else {
            // Otherwise take the class annotation
            annotation = sf.getDeclaringClass().getAnnotation(AKHQSecured.class);
        }

        return annotation;
    }

    protected void checkIfClusterAllowed(String cluster) {
        checkIfClusterAndResourceAllowed(cluster, StringUtils.EMPTY_STRING);
    }

    protected void checkIfClusterAndResourceAllowed(String cluster, List<String> resources) {
        for(String resource : resources) {
            checkIfClusterAndResourceAllowed(cluster, resource);
        }
    }

    protected void checkIfClusterAndResourceAllowed(String cluster, String resource) {
        // Authentication disabled, we allow everything
        if (!applicationContext.containsBean(SecurityService.class))
            return;

        boolean isAllowed;

        try {
            AKHQSecured annotation = getCallingAKHQSecuredAnnotation();
            Optional<Authentication> authentication = applicationContext.getBean(SecurityService.class).getAuthentication();

            if (authentication.isPresent() && isEmployeeAuthentication(authentication.get())) {
                isAllowed = getEmployeeEffectiveGrants(authentication.get()).stream()
                    .anyMatch(grant -> grant.resource() == annotation.resource()
                        && grant.action() == annotation.action()
                        && Pattern.matches(grant.clusterPattern(), cluster)
                        && (StringUtils.isEmpty(resource) || Pattern.matches(grant.topicPattern(), resource)));
            } else {
                isAllowed = getUserGroups().stream()
                    // Get only group with role matching the method annotation resource and action
                    .filter(groupBinding -> securityProperties.getRoles().entrySet().stream()
                        .filter(role -> groupBinding.getRole().equals(role.getKey()))
                        .flatMap(role -> role.getValue().stream())
                        .anyMatch(roleBinding -> roleBinding.getResources().contains(annotation.resource())
                            && roleBinding.getActions().contains(annotation.action())))
                    // Check that resource and cluster patterns match
                    .anyMatch(group -> {
                        boolean allowed = group.getClusters().stream()
                            .anyMatch(pattern -> Pattern.matches(pattern, cluster));

                        if (StringUtils.isNotEmpty(resource)) {
                            allowed = allowed && group.getPatterns().stream()
                                .anyMatch(pattern -> Pattern.matches(pattern, resource));
                        }

                        return allowed;
                    });
            }
        } catch (NoSuchMethodException e) {
            isAllowed = false;
        }

        if (!isAllowed) {
            throw new AuthorizationException(applicationContext.getBean(SecurityService.class).getAuthentication()
                .orElse(null));
        }
    }
}
