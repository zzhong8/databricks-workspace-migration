# Databricks notebook source
import warnings
import functools
import numpy as np
import tensorflow as tf
k, K = tf.keras, tf.keras.backend

try:
    from keras_radam import RAdam
    default_optimizer = RAdam(total_steps=10000)
except (ModuleNotFoundError, NameError):
    default_optimizer = k.optimizers.Adam()
    warnings.warn('Please consider installing `pip install keras-rectified-adam` for better optimization')

squeeze = functools.partial(K.squeeze, axis=-1)


class DenseDropout(k.Model):
    def __init__(self, units, rate=0.2, *args, **kwargs):
        activation = kwargs.pop('activation', None)

        super().__init__(*args, **kwargs)
        self.units = units
        self.rate = rate

        self.dropout = k.layers.Dropout(rate=self.rate)
        self.dense = k.layers.Dense(self.units, activation=activation)

    def call(self, inputs, training=True, mask=None):
        x = self.dropout(inputs, training=training)
        x = self.dense(x)
        return x


class EpsilonLayer(k.layers.Layer):
    def __init__(self, **kwargs):
        self.epsilon = None
        super().__init__(**kwargs)

    def build(self, input_shape):
        # Create a single trainable layer
        self.epsilon = self.add_weight(
            name='epsilon',
            shape=[1, 1],
            initializer=k.initializers.RandomNormal(),
            trainable='True'
        )
        super(EpsilonLayer, self).build(input_shape)

    def call(self, inputs, **kwargs):
        return self.epsilon * K.ones_like(inputs)[:, 0:1]


def create_conditional_average_treatment_effect(output_dim):
    is_multivariate = output_dim != 1

    def conditional_average_treatment_effect(_, concat_pred):
        """
        A metric for tracking the current estimate of the CATE where...

        CATE = [f(x|treatment) - f(x|control)].mean()

        :param concat_pred: [y|treatment, y|control, epsilon**, propensity, is_treatment]
               indices      [    :p     ,   p:2*p  ,   -3     ,     -2    ,     -1      ]
        :return:
        """
        y_pred_given_treatment = concat_pred[:, :output_dim]
        y_pred_given_control = concat_pred[:, output_dim:2 * output_dim]

        if is_multivariate is False:
            y_pred_given_treatment = squeeze(y_pred_given_treatment)
            y_pred_given_control = squeeze(y_pred_given_control)

        cate = K.mean(y_pred_given_treatment - y_pred_given_control, axis=-1)
        return cate

    return conditional_average_treatment_effect


def propensity_xentropy(_, concat_pred):
    """
    A metric for tracking how much progress the propensity score is making

    :param _: [y]
    :param concat_pred: [y|treatment, y|control, epsilon**, propensity, is_treatment]
               indices      [    :p     ,   p:2*p  ,   -3     ,     -2    ,     -1      ]
    """
    return K.mean(k.losses.binary_crossentropy(
        concat_pred[:, -1],  # True is treatment
        concat_pred[:, -2]  # Estimated propensity score
    ))


def treatment_accuracy(_, concat_pred):
    """
    Accuracy about the propensity score

    :param _: [y]
    :param concat_pred: [y|treatment, y|control, epsilon**, propensity, is_treatment]
               indices      [    :p     ,   p:2*p  ,   -3     ,     -2    ,     -1      ]
    :return:
    """
    return k.metrics.binary_accuracy(
        concat_pred[:, -1],  # True is treatment
        concat_pred[:, -2]  # Estimated propensity score
    )


def create_causal_regression_loss(output_dim, alpha, targeted_regularization: bool = False):
    """
    Create a loss function specific to y dimensionality

    concat_true: [y]
    concat_pred: [y|treatment, y|control, propensity, is_treatment]

    :param output_dim:
    :param targeted_regularization:
    :param alpha: The strength of targeted regularization
    :return:
    """
    is_multivariate = output_dim != 1

    def causal_regression_with_propensity_score(y_true, concat_pred):
        """
        Currently an exact implementation of the paper, therefore NN

        NOTE: epsilon is a optional param.

        :param y_true: [y]
        :param concat_pred: [y|treatment, y|control, epsilon**, propensity, is_treatment]
               indices      [    :p     ,   p:2*p  ,   -3     ,     -2    ,     -1      ]
        :return:
        """
        is_treatment = concat_pred[:, -1]
        propensity_hat = concat_pred[:, -2]

        if is_multivariate:
            is_treatment = tf.reshape(is_treatment, [-1, 1])
            propensity_hat = tf.reshape(propensity_hat, [-1, 1])

        propensity_score_entropy = K.mean(k.losses.binary_crossentropy(is_treatment, propensity_hat))

        y_pred_treatment = concat_pred[:, :output_dim]
        y_pred_control = concat_pred[:, output_dim:2 * output_dim]

        if not is_multivariate:
            y_true = squeeze(y_true)
            y_pred_treatment = squeeze(y_pred_treatment)
            y_pred_control = squeeze(y_pred_control)

        mse_treatment = K.mean(is_treatment * K.square(y_true - y_pred_treatment), axis=-1)
        mse_control = K.mean((1 - is_treatment) * K.square(y_true - y_pred_control), axis=-1)
        mse = mse_treatment + mse_control

        # Create the loss function
        if targeted_regularization:
            epsilon = concat_pred[:, -3]
            adjustment = epsilon * ((is_treatment / propensity_hat) - ((1 - is_treatment) / (1 - propensity_hat)))

            y_pred_adj_treatment = y_pred_treatment + adjustment
            y_pred_adj_control = y_pred_control + adjustment

            gamma_treatment = K.mean(is_treatment * K.square(y_true - y_pred_adj_treatment), axis=-1)
            gamma_control = K.mean((1 - is_treatment) * K.square(y_true - y_pred_adj_control), axis=-1)
            gamma = gamma_treatment + gamma_control

            j = mse + propensity_score_entropy + alpha * gamma
        else:
            j = mse + propensity_score_entropy
        return j

    return causal_regression_with_propensity_score


class JointCATEModel:
    # noinspection PyPep8Naming
    def __init__(self,
                 x, y, treatment,
                 z_size=25, variational=False,
                 targeted_regularization=False, alpha=1.0,
                 epochs=10000, validation_split=0.1, batch_size=1024,
                 optimizer=None,
                 verbose=False,
                 model_path='cate_model.nn'):
        self.z_size = z_size
        self.verbose = verbose
        self.model_path = model_path

        self.variational = variational
        self.targeted_regularization, self.alpha = targeted_regularization, alpha

        if self.variational is True and optimizer is None:
            optimizer = k.optimizers.SGD()
        elif self.variational is False and optimizer is None:
            optimizer = default_optimizer

        def adjust_units(n):
            return int((1 / (1 - 0.2)) * n) if variational else n  # Adjust N by the inverse dropout rate

        self.output_dim = 1 if y.ndim == 1 else y.shape[1]
        self.treatment_base_rate = max(treatment.mean(), 1 - treatment.mean())

        FCLayer = DenseDropout if self.variational else k.layers.Dense

        # Create model ####
        input_layer = k.layers.Input(shape=(x.shape[1],), name='X')
        is_treatment_label_layer = k.layers.Input(shape=(1,), name='isTreatment')

        # Shared representation
        z = FCLayer(adjust_units(75), activation='elu')(input_layer)
        z = FCLayer(adjust_units(z_size), activation='linear', name='Z')(z)

        # Propensity estimator (Shouldn't be dropped out
        propensity_hat = k.layers.Dense(1, activation='sigmoid', name='propensity_score')(z)

        # Counterfactual estimator
        h_treatment = FCLayer(adjust_units(75), activation='elu')(z)
        h_treatment = FCLayer(self.output_dim, name='H_treatment')(h_treatment)

        h_control = FCLayer(adjust_units(75), activation='elu')(z)
        h_control = FCLayer(self.output_dim, name='H_control')(h_control)

        # Extras
        ite_output = k.layers.Subtract(name='individual_treatment_effect')([h_treatment, h_control])

        if targeted_regularization:
            eps_value = EpsilonLayer(name='epsilon')(propensity_hat)
            concat_output = k.layers.concatenate([
                h_treatment, h_control, eps_value, propensity_hat, is_treatment_label_layer
            ])
        else:
            concat_output = k.layers.concatenate([h_treatment, h_control, propensity_hat, is_treatment_label_layer])

        # Models
        self.treatment_model = k.models.Model(input_layer, h_treatment, name='Treatment Model')
        self.control_model = k.models.Model(input_layer, h_control, name='Control Model')
        self.propensity_model = k.models.Model(input_layer, propensity_hat, name='Propensity Estimator')
        self.ite_model = k.models.Model(input_layer, ite_output, name='ITE Model')
        self.trainable_model = k.models.Model([input_layer, is_treatment_label_layer], concat_output)

        # Compile model
        if verbose:
            print(self.trainable_model.summary())

        self.trainable_model.compile(
            optimizer=optimizer,
            loss=create_causal_regression_loss(self.output_dim, self.alpha),
            metrics=[
                treatment_accuracy,
                propensity_xentropy,
                create_conditional_average_treatment_effect(self.output_dim)
            ]
        )

        # Train model
        callbacks = [
            k.callbacks.EarlyStopping(
                'val_loss',
                patience=500 if variational else 50
            ),
            k.callbacks.ReduceLROnPlateau(
                'val_loss',
                factor=0.5,
                patience=100 if variational else 10,
                min_lr=1e-6,
                verbose=verbose
            ),
            k.callbacks.TerminateOnNaN()
        ]
        if not variational:
            # TODO (8/27/2019) figure out how to create a proper config for the DenseDropout class
            callbacks.append(k.callbacks.ModelCheckpoint(model_path, save_best_only=True))

        self.summary = self.trainable_model.fit(
            [x, treatment], y,
            epochs=epochs, batch_size=x.shape[0] if x.shape[0] < batch_size else batch_size,
            validation_split=validation_split,
            callbacks=callbacks,
            verbose=max(int(verbose) - 1, 0)
        )

        # Check treatment accuracy
        if self.summary.history['treatment_accuracy'][-1] > 0.9:
            warnings.warn('Very strong selection bias detected! Overlap violations can harm quality results.')

    def compute_cate(self, x, samples=100):
        """
        CATE is computed by computing the mean of the individualized treatment effect of X.

        :param x:
        :param samples:
        :return:
        """
        if self.variational:
            from tqdm import trange
            return np.asarray([self.ite_model.predict(x).mean() for _ in trange(samples, desc='Sampling Posterior')])
        else:
            return self.ite_model.predict(x).mean()

    def plot_performance(self):
        import matplotlib.pyplot as graph

        graph.figure(figsize=(12, 6))
        graph.plot(self.summary.history['loss'], alpha=0.75, label=r'$J_{in}$')
        graph.plot(self.summary.history['val_loss'], alpha=0.75, label=r'$J_{out}$')
        graph.ylabel('Loss')
        graph.xlabel('Epochs')
        graph.legend()
        graph.show()

        graph.figure(figsize=(12, 6))
        graph.plot(self.summary.history['treatment_accuracy'], alpha=0.75, label='Training')
        graph.plot(self.summary.history['val_treatment_accuracy'], alpha=0.75, label='Validation')
        graph.axhline(
            self.treatment_base_rate,
            linestyle='--', color='black',
            label='Accuracy of Unconfoundedness'
        )
        graph.ylabel('Treatment Accuracy')
        graph.xlabel('Epochs')
        graph.legend()
        graph.show()

        graph.figure(figsize=(12, 6))
        graph.plot(self.summary.history['propensity_xentropy'], alpha=0.75, label='Training')
        graph.plot(self.summary.history['val_propensity_xentropy'], alpha=0.75, label='Validation')
        graph.ylabel('Propensity Loss (X-Entropy)')
        graph.xlabel('Epochs')
        graph.legend()
        graph.show()

    def plot_cate(self):
        import matplotlib.pyplot as graph

        graph.figure(figsize=(12, 6))
        graph.plot(self.summary.history['conditional_average_treatment_effect'], alpha=0.75, label='Training')
        graph.plot(self.summary.history['val_conditional_average_treatment_effect'], alpha=0.75, label='Validation')
        graph.ylabel('CATE')
        graph.xlabel('Epochs')
        graph.legend()
        graph.show()

    # noinspection PyBroadException
    @staticmethod
    def plot_model(model):
        k.utils.plot_model(model, 'tmp.svg', show_shapes=True)
        try:
            from IPython.display import display, SVG
            display(SVG('tmp.svg'))
        except Exception:
            print('Showing plots only works in jupyter')


class SplitCATEModel:
    def __init__(self):
        raise NotImplementedError


if __name__ == '__main__':
    from scipy import stats
    from scipy.special import expit
    import matplotlib.pyplot as graph
    import seaborn as sns

    # Setup quick fake data
    n, p = 5000, 20
    treatment_effect = 6

    m = stats.norm().rvs(p)
    x = stats.norm().rvs((n, p))

    # treatment_labels = stats.bernoulli(0.2).rvs(n)  # Unconfounded
    treatment_labels = expit((x[:, :5] @ m[:5]) + 2 * stats.norm().rvs(n)).round()  # Confounding

    y = x @ m + treatment_effect * treatment_labels + stats.norm().rvs(n)

    print(x.shape, y.shape, treatment_labels.shape)

    joint_cate_model = JointCATEModel(
        x, y, treatment_labels,
        epochs=2000,
        targeted_regularization=False,
        variational=False,
        verbose=2
    )

    joint_cate_model.plot_performance()
    joint_cate_model.plot_cate()

    graph.figure(figsize=(12, 5))
    if joint_cate_model.variational:
        graph.title(f'Distribution of ITE (CATE = {joint_cate_model.compute_cate(x).mean():2f})')
    else:
        graph.title(f'Distribution of ITE (CATE = {joint_cate_model.compute_cate(x):2f})')
    sns.distplot(joint_cate_model.ite_model.predict(x), label='Estimated Conditional ITE')
    graph.xlabel('ITE')
    graph.legend()
    graph.show()
