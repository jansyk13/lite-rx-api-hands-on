package io.pivotal.literx;

import org.junit.Test;

import io.pivotal.literx.domain.User;
import io.pivotal.literx.repository.ReactiveRepository;
import io.pivotal.literx.repository.ReactiveUserRepository;
import io.pivotal.literx.test.TestSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Learn how to merge flux.
 * @author Sebastien Deleuze
 */
public class Part04Merge {

    final static User MARIE = new User("mschrader", "Marie", "Schrader");
    final static User MIKE = new User("mehrmantraut", "Mike", "Ehrmantraut");

    ReactiveRepository<User> repository1 = new ReactiveUserRepository(500);
    ReactiveRepository<User> repository2 = new ReactiveUserRepository(MARIE, MIKE);

//========================================================================================

    @Test
    public void mergeWithInterleave() {
        Flux<User> flux = mergeFluxWithInterleave(repository1.findAll(), repository2.findAll());
        TestSubscriber
                .subscribe(flux)
                .await()
                .assertValues(MARIE, MIKE, User.SKYLER, User.JESSE, User.WALTER, User.SAUL)
                .assertComplete();
    }

    // TODO Merge flux1 and flux2 values with interleave
    Flux<User> mergeFluxWithInterleave(Flux<User> flux1, Flux<User> flux2) {
        Flux.merge(flux1, flux2);
        return flux1.mergeWith(flux2);
    }

//========================================================================================

    @Test
    public void mergeWithNoInterleave() {
        Flux<User> flux = mergeFluxWithNoInterleave(repository1.findAll(), repository2.findAll());
        TestSubscriber
                .subscribe(flux)
                .await()
                .assertValues(User.SKYLER, User.JESSE, User.WALTER, User.SAUL, MARIE, MIKE)
                .assertComplete();
    }

    // TODO Merge flux1 and flux2 values with no interleave (flux1 values, and then flux2 values)
    Flux<User> mergeFluxWithNoInterleave(Flux<User> flux1, Flux<User> flux2) {
        Flux.concat(flux1, flux2);
        return flux1.concat(flux2);
    }

//========================================================================================

    @Test
    public void multipleMonoToFlux() {
        Mono<User> skylerMono = repository1.findFirst();
        Mono<User> marieMono = repository2.findFirst();
        Flux<User> flux = createFluxFromMultipleMono(skylerMono, marieMono);
        TestSubscriber
                .subscribe(flux)
                .await()
                .assertValues(User.SKYLER, MARIE)
                .assertComplete();
    }

    // TODO Create a Flux containing the values of the 2 Mono
    Flux<User> createFluxFromMultipleMono(Mono<User> mono1, Mono<User> mono2) {
        return Flux.concat(mono1, mono2);
//                mono1.concatWith(mono2);
    }

}
