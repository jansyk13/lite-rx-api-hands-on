package io.pivotal.literx;

import org.junit.Test;

import io.pivotal.literx.domain.User;
import io.pivotal.literx.repository.ReactiveRepository;
import io.pivotal.literx.repository.ReactiveUserRepository;
import io.pivotal.literx.test.TestSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Learn how to transform values.
 * @author Sebastien Deleuze
 */
public class Part03Transform {

    ReactiveRepository<User> repository = new ReactiveUserRepository();

//========================================================================================

    @Test
    public void transformMono() {
        Mono<User> mono = repository.findFirst();
        TestSubscriber
                .subscribe(capitalizeOne(mono))
                .await()
                .assertValues(new User("SWHITE", "SKYLER", "WHITE"))
                .assertComplete();
    }

    // TODO Capitalize the user username, firstname and lastname
    Mono<User> capitalizeOne(Mono<User> mono) {
        return mono.map(user -> new User(user.getUsername().toUpperCase(), user.getFirstname().toUpperCase(), user.getLastname().toUpperCase()));
    }

//========================================================================================

    @Test
    public void transformFlux() {
        Flux<User> flux = repository.findAll();
        TestSubscriber
                .subscribe(capitalizeMany(flux))
                .await()
                .assertValues(
                        new User("SWHITE", "SKYLER", "WHITE"),
                        new User("JPINKMAN", "JESSE", "PINKMAN"),
                        new User("WWHITE", "WALTER", "WHITE"),
                        new User("SGOODMAN", "SAUL", "GOODMAN"))
                .assertComplete();
    }

    // TODO Capitalize the users username, firstName and lastName
    Flux<User> capitalizeMany(Flux<User> flux) {
        return flux.map(user -> new User(user.getUsername().toUpperCase(), user.getFirstname().toUpperCase(), user.getLastname().toUpperCase()));
    }

//========================================================================================

    @Test
    public void asyncTransformFlux() {
        Flux<User> flux = repository.findAll();
        TestSubscriber
                .subscribe(asyncCapitalizeMany(flux))
                .await()
                .assertValues(
                        new User("SWHITE", "SKYLER", "WHITE"),
                        new User("JPINKMAN", "JESSE", "PINKMAN"),
                        new User("WWHITE", "WALTER", "WHITE"),
                        new User("SGOODMAN", "SAUL", "GOODMAN"))
                .assertComplete();
    }

    // TODO Capitalize the users username, firstName and lastName using asyncCapitalizeUser()
    Flux<User> asyncCapitalizeMany(Flux<User> flux) {
        return flux.flatMap(u -> asyncCapitalizeUser(u));
    }

    Mono<User> asyncCapitalizeUser(User u) {
        return Mono.just(new User(u.getUsername().toUpperCase(), u.getFirstname().toUpperCase(), u.getLastname().toUpperCase()));
    }

}
