from __future__ import absolute_import
from keras import backend as K
from keras import initializers, regularizers, constraints
from keras.engine import Layer, InputSpec

def path_energy(y, x, U, b_start=None, b_end=None, mask=None):
    '''
    calculate the energy of y for x (with mask)
    transition energies U
    boundary energies b_start, b_end
    '''
    x = add_boundary_energy(x, b_start, b_end, mask)
    return path_energy0(y, x, U, mask)

def path_energy0(y, x, U, mask=None):
    '''
    energy without boundary potential hadling
    '''
    n_classes = K.shape(x)[2]
    y_one_hot = K.one_hot(y, n_classes)

    energy = K.sum(x * y_one_hot, 2)
    energy = K.sum(energy, 1)

    y_t = y[:, :-1]
    y_tp1 = y[:, 1:]
    U_flat = K.reshape(U, [-1])
    flat_indices = y_t * n_classes + y_tp1
    U_yt_tp1 = K.gather(U_flat, flat_indices)

    if mask is not None:
        mask = K.cast(mask, K.floatx())
        y_t_mask = mask[:, :-1]
        y_tp1_mask = mask[:, 1:]
        U_yt_tp1 *= y_t_mask * y_tp1_mask

    energy += K.sum(U_yt_tp1, axis=1)

    return energy 

def add_boundary_energy(x, b_start=None, b_end=None, mask=None):
    if mask is None:
        if b_start is not None:
            x = K.concatenate([x[:, :1, :] + b_start, x[:, 1:, :]], axis=1)
        if b_end is not None:
            x = K.concatenate([x[:, :-1, :], x[:, -1:, :] + b_end], axis=1)
    else:
        mask = K.cast(mask, K.floatx())
        mask = K.expand_dims(mask, 2)
        x *= mask
        if b_start is not None:
            mask_r = K.concatenate([K.zeros_like(mask[:, :1]), mask[:, :-1]], axis=1)
            start_mask = K.cast(K.greater(mask, mask_r), K.floatx())
            x = x + start_mask * b_start
        if b_end is not None:
            mask_l = K.concatenate([mask[:, 1:], K.zeros_like(mask[:, -1:])], axis=1)
            end_mask = K.cast(K.greater(mask, mask_l), K.floatx())
            x = x + end_mask * b_end
    return x

def free energy(x, U, b_start=None, b_end=None, mask=None):
    x = add_boundary_energy(x, b_start, b_end, mask)
    return free energy0(x, U, mask)

def free_energy0(x, U, mask=None):
    initial_states == [x[:, 0, :]]
    last_alpha, _ = _forward(x, lambda B: [K.logsumexp(B, axis=1)],initial_states, U, mask)
    return last_alpha[:, 0]

def _forward(x, reduce_step, initial_states, U, mask=None):
    def _forward_step(energy_matrix, states):
        alpha_tm1 = states[-1]
        new_states = reduce_step(K.expand_dims(alpha_tm1, 2) + energy_matrix)
        return new_states[0], new_states
    
    U_shared = K.expand_dims(K.expand_dims(U, 0), 0)

    if mask is not None:
        mask = K.cast(mask, K.floatx())
        mask_U = K.expand_dims(K.expand_dims(mask[:, :-1] * mask[:, 1:], 2), 3)
        U_shared = U_shared * mask_U
    
    inputs = K.expand_dims(x[:, 1:, :], 2) + U_shared
    inputs = K.concatenate([inputs, K.zeros_like(inputs[:, -1:, :, :])], axis=1)
    last, values, _ = K.rnn(_forward_step, inputs, initial_states)
    return last, values

def sparse_chain_crf_loss(y, x, U, b_start=None, b_end=None, mask=None):
    '''
    computes the loss function of Linear Chain Conditional Random Field

    loss(y,x) = NNL(P(y|x)), where P(y|x) = exp(E(y,x))/Z
    -> loss(y,x) = -E(y,x) + log(Z)
    
    E(y,x): the tag path energy
    Z: the normalization constant
    log(Z): free energy
    '''
    X = add_boundary_energy(x, b_start, b_end, mask)
    energy = path_energy0(y, x, U, mask)
    energy -= fre