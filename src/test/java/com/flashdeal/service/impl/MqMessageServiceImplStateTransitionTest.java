package com.flashdeal.service.impl;

import com.flashdeal.enums.MqMessageStatus;
import com.flashdeal.mapper.MqMessageMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MqMessageServiceImplStateTransitionTest {

    @Test
    void confirmAckShouldNotOverwriteReturnedOrTerminalStates() {
        Fixture fixture = new Fixture();

        fixture.service.markConfirmed(1L);

        Collection<String> statuses = captureStatuses(fixture.mapper);
        assertTrue(statuses.contains(MqMessageStatus.SEND_FAILED.name()));
        assertFalse(statuses.contains(MqMessageStatus.RETURNED.name()));
        assertFalse(statuses.contains(MqMessageStatus.CONSUMED.name()));
        assertFalse(statuses.contains(MqMessageStatus.FAILED.name()));
        assertFalse(statuses.contains(MqMessageStatus.NEED_MANUAL.name()));
    }

    @Test
    void returnShouldOverrideConfirmedButProtectTerminalStates() {
        Fixture fixture = new Fixture();

        fixture.service.markReturned(1L, "returned", LocalDateTime.now());

        Collection<String> statuses = captureStatuses(fixture.mapper);
        assertTrue(statuses.contains(MqMessageStatus.SEND_FAILED.name()));
        assertTrue(statuses.contains(MqMessageStatus.CONFIRMED.name()));
        assertFalse(statuses.contains(MqMessageStatus.CONSUMED.name()));
        assertFalse(statuses.contains(MqMessageStatus.FAILED.name()));
        assertFalse(statuses.contains(MqMessageStatus.NEED_MANUAL.name()));
    }

    @Test
    void resendSuccessShouldOnlyMoveClaimedRetryingMessageToSent() {
        Fixture fixture = new Fixture();

        fixture.service.markSentForRetry(1L);

        Collection<String> statuses = captureStatuses(fixture.mapper);
        assertTrue(statuses.contains(MqMessageStatus.RETRYING.name()));
        assertTrue(statuses.size() == 1);
    }

    private Collection<String> captureStatuses(MqMessageMapper mapper) {
        ArgumentCaptor<Collection<String>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(mapper).updateStatusIfIn(eq(1L), captor.capture(), any(), any(), any());
        return captor.getValue();
    }

    private static class Fixture {
        private final MqMessageMapper mapper = mock(MqMessageMapper.class);
        private final MqMessageServiceImpl service = new MqMessageServiceImpl();

        private Fixture() {
            ReflectionTestUtils.setField(service, "baseMapper", mapper);
            when(mapper.updateStatusIfIn(any(), anyCollection(), any(), any(), any())).thenReturn(1);
        }
    }
}
